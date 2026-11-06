from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from collections.abc import Awaitable, Callable, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any

from ua_outages.http import Fetcher, new_session, redact
from ua_outages.models import KYIV, DaySchedule, now_kyiv
from ua_outages.registry import OPERATORS, Operator

from outage_feed.alerts import (
    ALERTS_FALLBACK,
    ALERTS_ONLY,
    KEY_VARIABLE,
    SOURCE,
    Get,
    Outcome,
    alerts_days,
    live_get,
    run_pass,
)
from outage_feed.convert import to_yasno


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text("utf-8"))
    except (OSError, ValueError):
        return None


def _write_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, separators=(",", ":")), "utf-8")


async def _fetch_first(op: Operator, session: Any, now: datetime) -> tuple[str, dict[date, DaySchedule]] | None:
    answered: str | None = None
    for source in op.sources:
        if source.ukraine_only:
            continue
        try:
            days = await source.fetch(Fetcher(session, source.hosts), now)
        except Exception as err:
            print(f"{op.id}: {source.name} failed: {redact(f'{type(err).__name__}: {err}')}", file=sys.stderr)
            continue
        if days:
            return source.name, days
        print(f"{op.id}: {source.name} published nothing", file=sys.stderr)
        answered = answered or source.name
    return None if answered is None else (answered, {})


async def _fetch_within(
    op: Operator, session: Any, now: datetime, timeout: float
) -> tuple[str, dict[date, DaySchedule]] | None:
    try:
        return await asyncio.wait_for(_fetch_first(op, session, now), timeout)
    except TimeoutError:
        print(f"{op.id}: no answer within {timeout:g} s", file=sys.stderr)
        return None


def _failed_entry(old_index: dict, op_id: str) -> dict:
    old = old_index.get(op_id)
    old = old if isinstance(old, dict) else {}
    return {"updatedAt": old.get("updatedAt"), "source": old.get("source"), "ok": False}


def _keep_previous(op_id: str, out: Path, previous: Path) -> None:
    old_file = previous / f"{op_id}.json"
    if old_file.is_file():
        (out / old_file.name).write_bytes(old_file.read_bytes())


async def _alerts_pass(
    wanted: dict[str, str],
    alerts_get: Get | None,
    old_index: dict,
    previous: Path,
    now: datetime,
    timeout: float,
    clock: Callable[[], float],
    sleep: Callable[[float], Awaitable[None]],
) -> Outcome:
    carried = {k: old_index[k] for k in ("alertsCheckedAt", "alertsUpdated", "alertsRetryAfter") if k in old_index}
    key = os.environ.get(KEY_VARIABLE)
    if alerts_get is None and not key:
        print(f"{SOURCE}: {KEY_VARIABLE} is not set, skipping", file=sys.stderr)
        return Outcome(failed=set(wanted), meta=carried)
    async with new_session() as session:
        get = alerts_get or live_get(session, key or "")
        try:
            return await asyncio.wait_for(
                run_pass(wanted, get, old_index=old_index, previous=previous, now=now, clock=clock, sleep=sleep),
                timeout,
            )
        except TimeoutError:
            print(f"{SOURCE}: no answer within {timeout:g} s", file=sys.stderr)
            return Outcome(failed=set(wanted), meta={**carried, "alertsCheckedAt": now.isoformat()})


async def collect(
    operators: Sequence[Operator],
    out: Path,
    previous: Path,
    *,
    now: datetime | None = None,
    timeout: float = 120,
    alerts_get: Get | None = None,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> dict[str, Any]:
    now = (now or now_kyiv()).astimezone(KYIV)
    out.mkdir(parents=True, exist_ok=True)
    old_index = _read_json(previous / "index.json")
    if not isinstance(old_index, dict):
        old_index = {}
    polled = [
        op
        for op in operators
        if op.id in ALERTS_ONLY or (not op.by_address and any(not s.ukraine_only for s in op.sources))
    ]
    free = [op for op in polled if op.id not in ALERTS_ONLY]
    async with new_session() as session:
        results = await asyncio.gather(*(_fetch_within(op, session, now, timeout) for op in free))
    fetched = dict(zip((op.id for op in free), results, strict=True))

    wanted = {
        op.id: ALERTS_ONLY.get(op.id) or ALERTS_FALLBACK[op.id]
        for op in polled
        if op.id in ALERTS_ONLY or (op.id in ALERTS_FALLBACK and fetched[op.id] is None)
    }
    outcome = await _alerts_pass(wanted, alerts_get, old_index, previous, now, timeout, clock, sleep) if wanted else None

    index: dict[str, Any] = {}
    for op in polled:
        stamp = {"updatedAt": now.isoformat(), "source": SOURCE, "ok": True}
        if op.id in wanted and outcome is not None and op.id in outcome.rows:
            days = alerts_days(outcome.rows[op.id], now.date())
            _write_json(out / f"{op.id}.json", to_yasno(days, op.groups, now.date()))
            index[op.id] = stamp
        elif op.id in wanted and outcome is not None and op.id in outcome.unchanged:
            _keep_previous(op.id, out, previous)
            index[op.id] = stamp
        elif op.id in ALERTS_ONLY and outcome is not None and outcome.skipped:
            _keep_previous(op.id, out, previous)
            old = old_index.get(op.id)
            index[op.id] = old if isinstance(old, dict) else _failed_entry(old_index, op.id)
        elif (result := fetched.get(op.id)) is not None:
            source_name, days = result
            _write_json(out / f"{op.id}.json", to_yasno(days, op.groups, now.date()))
            index[op.id] = {"updatedAt": now.isoformat(), "source": source_name, "ok": True}
        else:
            index[op.id] = _failed_entry(old_index, op.id)
            _keep_previous(op.id, out, previous)
    if outcome is not None:
        index.update(outcome.meta)
    else:
        index.update({k: old_index[k] for k in ("alertsCheckedAt", "alertsUpdated", "alertsRetryAfter") if k in old_index})
    _write_json(out / "index.json", index)
    return index


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m outage_feed.collect")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--previous", type=Path, required=True)
    args = parser.parse_args(argv)
    index = asyncio.run(collect(OPERATORS, args.out, args.previous))
    for op_id, entry in index.items():
        if isinstance(entry, dict) and "ok" in entry:
            print(f"{op_id}: {'ok' if entry['ok'] else 'FAILED'} ({entry['source']})")


if __name__ == "__main__":
    main()
