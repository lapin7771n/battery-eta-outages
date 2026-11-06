from __future__ import annotations

from collections.abc import Sequence
from datetime import date, timedelta
from typing import Any

from ua_outages.models import EMERGENCY, DaySchedule, Outage, at

WAITING = "WaitingForSchedule"
APPLIES = "ScheduleApplies"
EMERGENCY_STATUS = "EmergencyShutdowns"


def _slot(outage: Outage, day: date) -> dict[str, Any] | None:
    start = outage.start.hour * 60 + outage.start.minute if outage.start.date() == day else 0
    end = outage.end.hour * 60 + outage.end.minute if outage.end.date() == day else 1440
    if outage.end.date() < day or outage.start.date() > day or start >= end:
        return None
    return {"start": start, "end": end, "type": "Possible" if outage.possible else "Definite"}


def _day(schedule: DaySchedule | None, group: str, day: date) -> dict[str, Any] | None:
    date_text = at(day, 0).isoformat()
    if schedule is None:
        return {"date": date_text, "status": WAITING, "slots": []}
    if group in schedule.groups:
        slots = [_slot(o, day) for o in schedule.outages(group)]
        return {"date": date_text, "status": APPLIES, "slots": [s for s in slots if s]}
    if schedule.unknown.get(group) == EMERGENCY:
        return {"date": date_text, "status": EMERGENCY_STATUS, "slots": []}
    return None


def to_yasno(days: dict[date, DaySchedule], groups: Sequence[str], today: date) -> dict[str, dict]:
    tomorrow = today + timedelta(days=1)
    result: dict[str, dict] = {}
    for group in groups:
        today_day = _day(days.get(today), group, today)
        tomorrow_day = _day(days.get(tomorrow), group, tomorrow)
        if today_day is not None and tomorrow_day is not None:
            result[group] = {"today": today_day, "tomorrow": tomorrow_day}
    return result
