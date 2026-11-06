from datetime import date, timedelta

from ua_outages.models import EMERGENCY, DaySchedule, Outage, at

from outage_feed.convert import to_yasno

TODAY = date(2026, 11, 6)
TOMORROW = TODAY + timedelta(days=1)


def schedule(day, outages):
    return DaySchedule(day, groups={"1.1": outages})


def outage(day, start, end, possible=False):
    return Outage(at(day, *start), at(day, *end), possible)


def slot(start, end, kind="Definite"):
    return {"start": start, "end": end, "type": kind}


def test_to_yasno():
    cases = [
        (
            "outage today, nothing published for tomorrow",
            {TODAY: schedule(TODAY, (outage(TODAY, (8, 0), (11, 30)),))},
            TODAY,
            ("ScheduleApplies", [slot(480, 690)]),
            ("WaitingForSchedule", []),
        ),
        (
            "possible outage",
            {TODAY: schedule(TODAY, (outage(TODAY, (8, 0), (11, 30), possible=True),))},
            TODAY,
            ("ScheduleApplies", [slot(480, 690, "Possible")]),
            ("WaitingForSchedule", []),
        ),
        (
            "queue with no outages means power all day",
            {TODAY: schedule(TODAY, ()), TOMORROW: schedule(TOMORROW, ())},
            TODAY,
            ("ScheduleApplies", []),
            ("ScheduleApplies", []),
        ),
        (
            "outage until midnight ends at 1440",
            {TODAY: schedule(TODAY, (outage(TODAY, (22, 0), (24, 0)),))},
            TODAY,
            ("ScheduleApplies", [slot(1320, 1440)]),
            ("WaitingForSchedule", []),
        ),
        (
            "outage across midnight is cut at the day edge",
            {
                TODAY: schedule(TODAY, (Outage(at(TODAY, 22), at(TOMORROW, 2)),)),
                TOMORROW: schedule(TOMORROW, (Outage(at(TODAY, 22), at(TOMORROW, 2)),)),
            },
            TODAY,
            ("ScheduleApplies", [slot(1320, 1440)]),
            ("ScheduleApplies", [slot(0, 120)]),
        ),
        (
            "queue the schedule does not mention waits",
            {TODAY: DaySchedule(TODAY, groups={"2.1": ()})},
            TODAY,
            ("WaitingForSchedule", []),
            ("WaitingForSchedule", []),
        ),
        (
            "unparsed queue waits, emergency queue says so",
            {TODAY: DaySchedule(TODAY, unknown={"1.1": "garbled"}), TOMORROW: DaySchedule(TOMORROW, unknown={"1.1": EMERGENCY})},
            TODAY,
            ("WaitingForSchedule", []),
            ("EmergencyShutdowns", []),
        ),
    ]
    for label, days, today, want_today, want_tomorrow in cases:
        group = to_yasno(days, ["1.1"], today)["1.1"]
        assert (group["today"]["status"], group["today"]["slots"]) == want_today, label
        assert (group["tomorrow"]["status"], group["tomorrow"]["slots"]) == want_tomorrow, label


def test_to_yasno_dates_carry_the_kyiv_offset_of_each_day():
    group = to_yasno({}, ["1.1"], date(2025, 10, 26))["1.1"]

    assert group["today"]["date"] == "2025-10-26T00:00:00+03:00"
    assert group["tomorrow"]["date"] == "2025-10-27T00:00:00+02:00"


def test_to_yasno_returns_every_requested_queue():
    result = to_yasno({}, ["1.1", "3.2"], TODAY)

    assert list(result) == ["1.1", "3.2"]
