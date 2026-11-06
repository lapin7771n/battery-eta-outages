from __future__ import annotations

import asyncio
import json
import math
import sys
import time
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import aiohttp
from ua_outages.http import MAX_BYTES, USER_AGENT, redact, ssl_context
from ua_outages.models import EMERGENCY, DaySchedule, Outage, at, merge_outages

SOURCE = "alerts.energy"
BASE_URL = "https://alerts.energy/api/v1"
KEY_VARIABLE = "ALERTS_ENERGY_KEY"

#: Operators whose only source is alerts.energy: operator id -> area slug.
ALERTS_ONLY = {
    "kharkiv-oe": "kharkivska-oblast",
    "poltava-oe": "poltavska-oblast",
    "zaporizhzhia-oe": "zaporizka-oblast",
    "chernivtsi-oe": "chernivecka-oblast",
}
#: Operators that read alerts.energy as the last source, when every free source failed.
ALERTS_FALLBACK = {
    "dtek-krem": "kyivska-oblast",
    "zhytomyr-oe": "zhytomyrska-oblast",
    "kirovohrad-oe": "kirovogradska-oblast",
    "mykolaiv-oe": "mikolayivska-oblast",
    "dtek-oem": "odeska-oblast",
    "sumy-oe": "sumska-oblast",
    "cherkasy-oe": "cherkaska-oblast",
}

PASS_EVERY = timedelta(minutes=30)
#: GitHub starts cron runs late by varying amounts, so a run a little under ``PASS_EVERY`` still passes.
PASS_SLACK = timedelta(minutes=5)
#: An area fetched this long ago is fetched again even if the overview shows no change.
MAX_AGE = timedelta(hours=2)
MIN_GAP = 0.2
MIN_DAILY_REMAINING = 2000
DEFAULT_RETRY_AFTER = 60
OVERVIEW = "/source-registry/areas/overview"
OBLENERGO = "_oblenergo"
#: Overview fields whose change means an area's shutdowns changed.
CHANGE_FIELDS = ("updated", "mode", "has_gpv", "gpv_cancelled", "emergency_scope")
#: ``index.json`` keys the alerts pass keeps between runs.
META_KEYS = ("alertsCheckedAt", "alertsAreas", "alertsRetryAfter")

_WHOLE, _FIRST_HALF, _SECOND_HALF = 1, 2, 3


class AlertsError(Exception):
    pass


@dataclass(frozen=True)
class Reply:
    status: int
    #: Header names in lower case.
    headers: Mapping[str, str]
    body: Any


Get = Callable[[str], Awaitable[Reply]]


def live_get(session: Any, key: str) -> Get:
    async def get(path: str) -> Reply:
        try:
            async with session.request(
                "GET",
                BASE_URL + path,
                headers={"Authorization": f"Bearer {key}", "Accept": "application/json", "User-Agent": USER_AGENT},
                timeout=aiohttp.ClientTimeout(total=20),
                allow_redirects=False,
                ssl=ssl_context(),
            ) as resp:
                headers = {name.lower(): value for name, value in resp.headers.items()}
                if resp.status != 200:
                    return Reply(resp.status, headers, None)
                raw = await resp.content.read(MAX_BYTES + 1)
                if len(raw) > MAX_BYTES:
                    raise AlertsError(f"GET {path}: response is over {MAX_BYTES} bytes")
                return Reply(resp.status, headers, json.loads(raw))
        except (aiohttp.ClientError, TimeoutError, ValueError) as err:
            raise AlertsError(f"GET {path}: {type(err).__name__}") from err

    return get


def oblenergo_rows(rows: Iterable[Any]) -> list[dict]:
    """Rows of the regional operator; the railway's (``_uz``) and anyone else's are dropped."""
    return [r for r in rows if isinstance(r, dict) and str(r.get("initiator", "")).endswith(OBLENERGO)]


def _day(value: Any) -> date | None:
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def _hours(day: date, codes: Any) -> list[Outage] | None:
    if not isinstance(codes, list) or len(codes) != 24 or not all(c in (0, 1, 2, 3) for c in codes):
        return None
    outages = []
    for hour, code in enumerate(codes):
        if code == _WHOLE:
            outages.append(Outage(at(day, hour), at(day, hour + 1)))
        elif code == _FIRST_HALF:
            outages.append(Outage(at(day, hour), at(day, hour, 30)))
        elif code == _SECOND_HALF:
            outages.append(Outage(at(day, hour, 30), at(day, hour + 1)))
    return outages


def _possible(outages: list[Outage]) -> list[Outage]:
    return [Outage(o.start, o.end, possible=True) for o in outages]


def alerts_days(rows: Iterable[dict], today: date) -> dict[date, DaySchedule]:
    """Schedules by calendar day from rows of ``/source-registry/areas/{slug}/shutdowns``.

    A day is taken from the row's own ``today_date`` / ``tomorrow_date``; days before ``today`` are
    dropped. A queue the row gives no usable plan for is left out of the day, and a day with no
    queue at all is left out of the result.
    """
    groups: dict[date, dict[str, tuple[Outage, ...]]] = {}
    unknown: dict[date, dict[str, str]] = {}
    for row in rows:
        queue = row.get("queue")
        if not isinstance(queue, str):
            continue
        for part in ("today", "tomorrow"):
            day = _day(row.get(f"{part}_date"))
            if day is None or day < today:
                continue
            status = row.get(f"{part}_status")
            if status == "EmergencyShutdowns":
                unknown.setdefault(day, {})[queue] = EMERGENCY
            elif status == "NoShutdowns":
                groups.setdefault(day, {})[queue] = ()
            elif status == "ScheduleApplies":
                definite = _hours(day, row.get(part))
                possible = _hours(day, row.get(f"{part}_possible") or [0] * 24)
                if definite is not None and possible is not None:
                    groups.setdefault(day, {})[queue] = merge_outages(definite + _possible(possible))
    return {
        day: DaySchedule(day, groups=groups.get(day, {}), unknown=unknown.get(day, {}))
        for day in sorted(groups.keys() | unknown.keys())
    }


def _when(value: Any) -> datetime | None:
    try:
        return datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def _remaining(reply: Reply) -> int | None:
    try:
        return int(reply.headers["x-ratelimit-daily-remaining"])
    except (KeyError, ValueError):
        return None


class _Paced:
    """Requests one after another, at least ``MIN_GAP`` seconds apart."""

    def __init__(self, get: Get, clock: Callable[[], float], sleep: Callable[[float], Awaitable[Any]]) -> None:
        self._get = get
        self._clock = clock
        self._sleep = sleep
        self._last: float | None = None

    async def __call__(self, path: str) -> Reply:
        if self._last is not None and (wait := self._last + MIN_GAP - self._clock()) > 0:
            await self._sleep(wait)
        try:
            return await self._get(path)
        finally:
            self._last = self._clock()


@dataclass
class Outcome:
    """What one alerts pass learned, by operator id."""

    skipped: bool = False
    rows: dict[str, list[dict]] = field(default_factory=dict)
    unchanged: set[str] = field(default_factory=set)
    failed: set[str] = field(default_factory=set)
    #: Entries for ``index.json``.
    meta: dict[str, Any] = field(default_factory=dict)


def _has_today(path: Path, today: date) -> bool:
    try:
        feed = json.loads(path.read_text("utf-8"))
        return any(str(g["today"]["date"]).startswith(today.isoformat()) for g in feed.values())
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        return False


def _change_keys(body: Any) -> dict[str, list] | None:
    """Change key by area slug from the overview reply, or ``None`` when the reply is not a list."""
    if not isinstance(body, list):
        print(f"{SOURCE}: overview is {type(body).__name__}, not a list", file=sys.stderr)
        return None
    return {
        a["slug"]: [a.get(name) for name in CHANGE_FIELDS]
        for a in body
        if isinstance(a, dict) and isinstance(a.get("slug"), str) and isinstance(a.get("updated"), str)
    }


def _fresh(stored: Any, key: list, now: datetime) -> bool:
    if not isinstance(stored, dict) or stored.get("change") != key:
        return False
    fetched = _when(stored.get("fetchedAt"))
    return fetched is not None and now - fetched < MAX_AGE


async def run_pass(
    wanted: Mapping[str, str],
    get: Get,
    *,
    old_index: Mapping[str, Any],
    previous: Path,
    now: datetime,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], Awaitable[Any]] = asyncio.sleep,
    started: float | None = None,
    deadline: float = math.inf,
) -> Outcome:
    """One pass over the areas of ``wanted`` (operator id -> area slug).

    It asks for the overview once, then for the shutdowns of each area whose overview fields changed,
    whose last fetch is ``MAX_AGE`` old or that has no file for today. It does nothing when the last
    pass was under ``PASS_EVERY - PASS_SLACK`` ago or alerts.energy asked us to wait. A 429, a low daily
    quota or passing ``deadline`` (on ``clock``) ends the pass; what it fetched so far is kept.
    ``started`` is the ``clock`` reading taken with ``now``.
    """
    started = clock() if started is None else started
    meta = {k: old_index[k] for k in META_KEYS if k in old_index}
    checked, retry_after = _when(old_index.get("alertsCheckedAt")), _when(old_index.get("alertsRetryAfter"))
    if (checked and now - checked < PASS_EVERY - PASS_SLACK) or (retry_after and now < retry_after):
        return Outcome(skipped=True, meta=meta)

    outcome = Outcome(meta=meta)
    meta["alertsCheckedAt"] = now.isoformat()
    meta.pop("alertsRetryAfter", None)
    stored = old_index.get("alertsAreas")
    seen: dict[str, Any] = dict(stored) if isinstance(stored, dict) else {}
    meta["alertsAreas"] = seen
    call = _Paced(get, clock, sleep)
    stopped = False

    async def ask(path: str) -> Reply | None:
        nonlocal stopped
        if clock() >= deadline:
            stopped = True
            print(f"{SOURCE}: out of time before {path}, stopping", file=sys.stderr)
            return None
        try:
            reply = await call(path)
        except AlertsError as err:
            print(f"{SOURCE}: {redact(str(err))}", file=sys.stderr)
            return None
        if reply.status == 429:
            try:
                seconds = max(int(reply.headers["retry-after"]), 0)
            except (KeyError, ValueError):
                seconds = DEFAULT_RETRY_AFTER
            meta["alertsRetryAfter"] = (now + timedelta(seconds=clock() - started + seconds)).isoformat()
            stopped = True
            print(f"{SOURCE}: HTTP 429 on {path}, stopping", file=sys.stderr)
            return None
        if (left := _remaining(reply)) is not None and left < MIN_DAILY_REMAINING:
            stopped = True
            print(f"{SOURCE}: {left} requests left today, stopping", file=sys.stderr)
        if reply.status != 200:
            print(f"{SOURCE}: HTTP {reply.status} on {path}", file=sys.stderr)
            return None
        return reply

    overview = await ask(OVERVIEW)
    keys = _change_keys(overview.body) if overview is not None else None
    if keys is None:
        outcome.failed = set(wanted)
        return outcome

    for op_id, slug in wanted.items():
        key = keys.get(slug)
        old = old_index.get(op_id)
        if key is None:
            print(f"{op_id}: {SOURCE} overview has no updated time for {slug}", file=sys.stderr)
            outcome.failed.add(op_id)
            continue
        if (
            _fresh(seen.get(slug), key, now)
            and isinstance(old, dict)
            and old.get("source") == SOURCE
            and _has_today(previous / f"{op_id}.json", now.date())
        ):
            outcome.unchanged.add(op_id)
            continue
        if stopped:
            outcome.failed.add(op_id)
            continue
        reply = await ask(f"/source-registry/areas/{slug}/shutdowns")
        own = oblenergo_rows(reply.body) if reply is not None and isinstance(reply.body, list) else []
        if not own:
            if reply is not None:
                print(f"{op_id}: {SOURCE} has no {OBLENERGO} rows for {slug}", file=sys.stderr)
            outcome.failed.add(op_id)
            continue
        seen[slug] = {"change": key, "fetchedAt": now.isoformat()}
        outcome.rows[op_id] = own
    return outcome
