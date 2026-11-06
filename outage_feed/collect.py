from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections.abc import Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any

from ua_outages.http import Fetcher, new_session, redact
from ua_outages.models import KYIV, DaySchedule, now_kyiv
from ua_outages.registry import OPERATORS, Operator

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


async def collect(
    operators: Sequence[Operator], out: Path, previous: Path, *, now: datetime | None = None, timeout: float = 120
) -> dict[str, dict]:
    now = (now or now_kyiv()).astimezone(KYIV)
    out.mkdir(parents=True, exist_ok=True)
    old_index = _read_json(previous / "index.json")
    if not isinstance(old_index, dict):
        old_index = {}
    polled = [op for op in operators if not op.by_address and any(not s.ukraine_only for s in op.sources)]
    async with new_session() as session:
        results = await asyncio.gather(*(_fetch_within(op, session, now, timeout) for op in polled))
    index: dict[str, dict] = {}
    for op, fetched in zip(polled, results, strict=True):
        if fetched is not None:
            source_name, days = fetched
            _write_json(out / f"{op.id}.json", to_yasno(days, op.groups, now.date()))
            index[op.id] = {"updatedAt": now.isoformat(), "source": source_name, "ok": True}
            continue
        old = old_index.get(op.id) or {}
        index[op.id] = {"updatedAt": old.get("updatedAt"), "source": old.get("source"), "ok": False}
        old_file = previous / f"{op.id}.json"
        if old_file.is_file():
            (out / old_file.name).write_bytes(old_file.read_bytes())
    _write_json(out / "index.json", index)
    return index


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m outage_feed.collect")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--previous", type=Path, required=True)
    args = parser.parse_args(argv)
    index = asyncio.run(collect(OPERATORS, args.out, args.previous))
    for op_id, entry in index.items():
        print(f"{op_id}: {'ok' if entry['ok'] else 'FAILED'} ({entry['source']})")


if __name__ == "__main__":
    main()
