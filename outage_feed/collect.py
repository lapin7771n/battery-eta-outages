from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any

from ua_outages.http import Fetcher, Response, new_session, redact
from ua_outages.models import KYIV, DaySchedule, now_kyiv
from ua_outages.providers.base import Kind, Provider
from ua_outages.providers.dtek import DtekPage, OutageDataUa
from ua_outages.providers.yasno import Yasno
from ua_outages.registry import OPERATORS, Operator

from outage_feed.alerts import (
    ALERTS_AREAS,
    ALERTS_FALLBACK,
    ALERTS_IF_EMPTY,
    ALERTS_ONLY,
    KEY_VARIABLE,
    META_KEYS,
    SOURCE,
    Area,
    Get,
    Outcome,
    alerts_days,
    live_get,
    run_pass,
)
from outage_feed.convert import WAITING, to_yasno


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text("utf-8"))
    except (OSError, ValueError):
        return None


def _write_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, separators=(",", ":")), "utf-8")


_CHANNEL_RE = re.compile(r"[A-Za-z0-9_]+")


def _channel(source: Provider) -> str | None:
    channel = getattr(source, "channel", None)
    if channel is None and source.kind is Kind.TELEGRAM and source.page:
        channel = source.page.rstrip("/").rsplit("/", 1)[-1]
    return channel if isinstance(channel, str) and _CHANNEL_RE.fullmatch(channel) else None


#: The ids the app accepts.
SOURCE_ID_RE = re.compile(r"[a-z0-9-]{2,40}")


def source_id(source: Provider) -> str:
    """The id in ``<operator>@<id>.json``; ``collect`` skips a source whose id breaks ``SOURCE_ID_RE``."""
    if isinstance(source, Yasno):
        return "yasno"
    if isinstance(source, DtekPage):
        return "dtek-site"
    if isinstance(source, OutageDataUa):
        return "dtek-mirror"
    if source.kind is Kind.TELEGRAM:
        channel = _channel(source)
        return f"telegram-{channel.lower().replace('_', '-')}" if channel else "telegram"
    return "operator"


def source_label(source: Provider) -> str:
    """The short name the app shows for a source."""
    sid = source_id(source)
    if sid == "yasno":
        return "YASNO"
    if sid == "dtek-site":
        return "Сайт ДТЕК"
    if sid == "dtek-mirror":
        return "Копія ДТЕК (outage-data-ua)"
    if source.kind is Kind.TELEGRAM:
        channel = _channel(source)
        return f"Telegram: {channel}" if channel else "Telegram"
    return "Сайт обленерго"


def _queue_order(queue: str) -> tuple[int, int] | None:
    """Sort key of a queue key the way ``FeedClient.groups`` orders them; ``None`` for other keys."""
    parts = queue.split(".")
    if not 1 <= len(parts) <= 2 or not all(p.isascii() and p.isdigit() for p in parts):
        return None
    group, subgroup = int(parts[0]), int(parts[1]) if len(parts) == 2 else 0
    return (group, subgroup) if queue == (f"{group}.{subgroup}" if subgroup else str(group)) else None


def _queues(feed: Any) -> list[str]:
    if not isinstance(feed, dict):
        return []
    ordered = [(order, queue) for queue in feed if isinstance(queue, str) and (order := _queue_order(queue))]
    return [queue for _, queue in sorted(ordered)]


class _SharedFetcher(Fetcher):
    """A ``Fetcher`` whose GET shares one request with every identical GET of the run.

    ``dtek-dnem`` and ``cek`` read the same DTEK page and mirror, each with its own group map.
    """

    def __init__(
        self, session: Any, hosts: frozenset[str], shared: dict[tuple[Any, ...], asyncio.Task[Response]]
    ) -> None:
        super().__init__(session, hosts)
        self._allowed_hosts = hosts
        self._shared = shared

    async def request(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, str] | None = None,
        data: Mapping[str, str] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Response:
        if method != "GET" or data:
            return await super().request(method, url, params=params, data=data, headers=headers)
        key = (self._allowed_hosts, url, tuple(sorted((params or {}).items())), tuple(sorted((headers or {}).items())))
        task = self._shared.get(key)
        if task is None:
            task = asyncio.ensure_future(super().request(method, url, params=params, headers=headers))
            self._shared[key] = task
        # One source timing out must not cancel the request another source waits for.
        return await asyncio.shield(task)


async def _fetch_source(op: Operator, source: Provider, http: Fetcher, now: datetime, timeout: float) -> dict | None:
    """Days the source published (maybe none), or ``None`` when it failed or did not answer in time."""
    try:
        days = await asyncio.wait_for(source.fetch(http, now), timeout)
    except TimeoutError:
        print(f"{op.id}: {source.name} gave no answer within {timeout:g} s", file=sys.stderr)
        return None
    except Exception as err:
        print(f"{op.id}: {source.name} failed: {redact(f'{type(err).__name__}: {err}')}", file=sys.stderr)
        return None
    if not days:
        print(f"{op.id}: {source.name} published nothing", file=sys.stderr)
    return days


def _pick(answers: Sequence[tuple[Provider, dict | None]]) -> tuple[str, dict[date, DaySchedule]] | None:
    """The first source with days; else the first that answered with none; else ``None``."""
    answered: str | None = None
    for source, days in answers:
        if days:
            return source.name, days
        if days is not None:
            answered = answered or source.name
    return None if answered is None else (answered, {})


def _has_plan(feed: dict[str, dict]) -> bool:
    return any(day["status"] != WAITING for days in feed.values() for day in days.values())


def _failed_entry(old_index: dict, op_id: str) -> dict:
    old = old_index.get(op_id)
    old = old if isinstance(old, dict) else {}
    return {"updatedAt": old.get("updatedAt"), "source": old.get("source"), "ok": False}


def _old_sources(old_index: dict, op_id: str) -> dict[str, dict]:
    old = old_index.get(op_id)
    listed = old.get("sources") if isinstance(old, dict) else None
    if not isinstance(listed, list):
        return {}
    return {e["id"]: e for e in listed if isinstance(e, dict) and isinstance(e.get("id"), str)}


def _keep_previous(op_id: str, out: Path, previous: Path) -> None:
    old_file = previous / f"{op_id}.json"
    if old_file.is_file():
        (out / old_file.name).write_bytes(old_file.read_bytes())


def _source_entry(sid: str, name: str, ok: bool, updated: Any, feed: Any) -> dict:
    return {"id": sid, "name": name, "ok": ok, "updatedAt": updated, "queues": _queues(feed)}


def _publish_source(
    out: Path, previous: Path, op_id: str, sid: str, name: str, feed: dict | None, now: datetime, old: dict | None
) -> dict | None:
    """Write ``<op>@<sid>.json`` from ``feed`` (``None``: the source failed, so keep last run's file).

    Returns the ``index.json`` entry, or ``None`` when the source has no file.
    """
    file = f"{op_id}@{sid}.json"
    if feed is not None:
        _write_json(out / file, feed)
        return _source_entry(sid, name, True, now.isoformat(), feed)
    kept = _read_json(previous / file)
    if kept is None:
        return None
    (out / file).write_bytes((previous / file).read_bytes())
    return _source_entry(sid, name, False, (old or {}).get("updatedAt"), kept)


async def _alerts_pass(
    wanted: dict[str, Area],
    alerts_get: Get | None,
    old_index: dict,
    previous: Path,
    now: datetime,
    started: float,
    timeout: float,
    clock: Callable[[], float],
    sleep: Callable[[float], Awaitable[None]],
) -> Outcome:
    carried = {k: old_index[k] for k in META_KEYS if k in old_index}
    key = os.environ.get(KEY_VARIABLE)
    if alerts_get is None and not key:
        print(f"{SOURCE}: {KEY_VARIABLE} is not set, skipping", file=sys.stderr)
        return Outcome(failed=set(wanted), meta=carried)
    async with new_session() as session:
        get = alerts_get or live_get(session, key or "")
        return await run_pass(
            wanted,
            get,
            old_index=old_index,
            previous=previous,
            now=now,
            clock=clock,
            sleep=sleep,
            started=started,
            deadline=clock() + timeout,
        )


async def collect(
    operators: Sequence[Operator],
    out: Path,
    previous: Path,
    *,
    now: datetime | None = None,
    timeout: float = 120,
    alerts_timeout: float = 120,
    alerts_get: Get | None = None,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> dict[str, Any]:
    now = (now or now_kyiv()).astimezone(KYIV)
    today = now.date()
    started = clock()
    out.mkdir(parents=True, exist_ok=True)
    old_index = _read_json(previous / "index.json")
    if not isinstance(old_index, dict):
        old_index = {}
    polled = [
        op
        for op in operators
        if op.id in ALERTS_ONLY or (not op.by_address and any(not s.ukraine_only for s in op.sources))
    ]
    jobs = [(op, s) for op in polled if op.id not in ALERTS_ONLY for s in op.sources if not s.ukraine_only]
    wanted = {op.id: ALERTS_AREAS[op.id] for op in polled if op.id in ALERTS_AREAS}

    async def fetch_all() -> list[dict | None]:
        shared: dict[tuple[Any, ...], asyncio.Task[Response]] = {}
        async with new_session() as session:
            try:
                return await asyncio.gather(
                    *(_fetch_source(op, s, _SharedFetcher(session, s.hosts, shared), now, timeout) for op, s in jobs)
                )
            finally:
                pending = [task for task in shared.values() if not task.done()]
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)

    async def alerts() -> Outcome | None:
        if not wanted:
            return None
        return await _alerts_pass(wanted, alerts_get, old_index, previous, now, started, alerts_timeout, clock, sleep)

    answers, outcome = await asyncio.gather(fetch_all(), alerts())
    fetched: dict[str, list[tuple[Provider, dict | None]]] = {}
    for (op, source), days in zip(jobs, answers, strict=True):
        fetched.setdefault(op.id, []).append((source, days))

    index: dict[str, Any] = {}
    for op in polled:
        old = old_index.get(op.id)
        old_sources = _old_sources(old_index, op.id)
        entries: list[dict] = []
        taken: set[str] = set()
        for source, days in fetched.get(op.id, []):
            sid = source_id(source)
            if not SOURCE_ID_RE.fullmatch(sid) or sid in taken:
                print(f"{op.id}: {source.name} has an invalid or repeated id {sid!r}, not published", file=sys.stderr)
                continue
            taken.add(sid)
            feed = None if days is None else to_yasno(days, op.groups, today)
            entry = _publish_source(out, previous, op.id, sid, source_label(source), feed, now, old_sources.get(sid))
            if entry:
                entries.append(entry)
        result = _pick(fetched.get(op.id, []))

        state = None
        if op.id in wanted and outcome is not None:
            state = (
                "rows"
                if op.id in outcome.rows
                else "unchanged"
                if op.id in outcome.unchanged
                else "skipped"
                if outcome.skipped
                else "failed"
            )
        if state is not None:
            old_entry = old_sources.get("alerts")
            alerts_feed = None
            if state == "rows" and outcome is not None:
                alerts_feed = to_yasno(alerts_days(outcome.rows[op.id], today), op.groups, today)
            elif state == "unchanged":
                # Nothing changed, so last run's file stands for this run's data too.
                kept = _read_json(previous / f"{op.id}@alerts.json")
                alerts_feed = kept if isinstance(kept, dict) else None
            entry = _publish_source(out, previous, op.id, "alerts", SOURCE, alerts_feed, now, old_entry)
            if entry and state == "skipped" and old_entry:
                entry = {**entry, "ok": old_entry.get("ok") is True, "updatedAt": old_entry.get("updatedAt")}
            if entry:
                entries.append(entry)

        own_empty = result is None or (
            op.id in ALERTS_IF_EMPTY and not _has_plan(to_yasno(result[1], op.groups, today))
        )
        use_alerts = op.id in ALERTS_ONLY or (op.id in ALERTS_FALLBACK and own_empty)
        stamp = {"updatedAt": now.isoformat(), "source": SOURCE, "ok": True}
        if use_alerts and state in ("rows", "unchanged"):
            alerts_file = out / f"{op.id}@alerts.json"
            (out / f"{op.id}.json").write_bytes(alerts_file.read_bytes())
            index[op.id] = stamp
        elif (
            use_alerts
            and state == "skipped"
            and (op.id in ALERTS_ONLY or (isinstance(old, dict) and old.get("source") == SOURCE))
        ):
            _keep_previous(op.id, out, previous)
            index[op.id] = old if isinstance(old, dict) else _failed_entry(old_index, op.id)
        elif result is not None:
            source_name, days = result
            _write_json(out / f"{op.id}.json", to_yasno(days, op.groups, today))
            index[op.id] = {"updatedAt": now.isoformat(), "source": source_name, "ok": True}
        else:
            index[op.id] = _failed_entry(old_index, op.id)
            _keep_previous(op.id, out, previous)
        index[op.id] = {**index[op.id], "sources": entries}
    if outcome is not None:
        index.update(outcome.meta)
    else:
        index.update({k: old_index[k] for k in META_KEYS if k in old_index})
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
