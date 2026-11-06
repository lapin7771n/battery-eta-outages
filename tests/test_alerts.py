import asyncio
import json
from datetime import date, timedelta
from pathlib import Path

import aiohttp
import pytest
from ua_outages.models import EMERGENCY, at

from outage_feed.alerts import AlertsError, alerts_days, live_get, oblenergo_rows
from outage_feed.convert import to_yasno

TODAY = date(2026, 11, 6)
TOMORROW = TODAY + timedelta(days=1)
FIXTURES = Path(__file__).parent / "fixtures" / "alerts"


def fixture(name):
    return json.loads((FIXTURES / f"shutdowns-{name}-oblast.json").read_text("utf-8"))


def row(queue="1.1", today=None, today_possible=None, status="ScheduleApplies", **more):
    return {
        "initiator": "x_oblast_oblenergo",
        "queue": queue,
        "today": [0] * 24 if today is None else today,
        "today_possible": today_possible if today_possible is not None else [0] * 24,
        "tomorrow": [],
        "today_date": TODAY.isoformat(),
        "tomorrow_date": TOMORROW.isoformat(),
        "today_status": status,
        "tomorrow_status": "WaitingForSchedule",
        **more,
    }


def hours(**codes):
    """24 hour codes; ``h5=1`` sets hour 5 to code 1."""
    result = [0] * 24
    for name, code in codes.items():
        result[int(name[1:])] = code
    return result


def spans(schedule, queue="1.1"):
    midnight = at(schedule.day, 0)
    return [
        ((o.start - midnight) // timedelta(minutes=1), (o.end - midnight) // timedelta(minutes=1), o.possible)
        for o in schedule.outages(queue)
    ]


def test_alerts_days():
    cases = [
        ("code 0 is power", hours(), hours(), []),
        ("code 1 is the whole hour", hours(h1=1), hours(), [(60, 120, False)]),
        ("code 2 is the first half", hours(h2=2), hours(), [(120, 150, False)]),
        ("code 3 is the second half", hours(h3=3), hours(), [(210, 240, False)]),
        (
            "adjacent hours and halves merge",
            hours(h3=1, h4=1, h5=2, h6=3),
            hours(),
            [(180, 330, False), (390, 420, False)],
        ),
        ("midnight ends the day", hours(h22=1, h23=1), hours(), [(1320, 1440, False)]),
        ("possible codes make possible outages", hours(), hours(h8=1, h9=2), [(480, 570, True)]),
        (
            "a definite outage wins over a possible one",
            hours(h8=1),
            hours(h8=1, h9=1),
            [(480, 540, False), (540, 600, True)],
        ),
    ]
    for name, definite, possible, expected in cases:
        days = alerts_days([row(today=definite, today_possible=possible)], TODAY)
        assert spans(days[TODAY]) == expected, name


def test_alerts_days_statuses_and_dates():
    rows = [
        row("applies", hours(h1=1)),
        row("none", status="NoShutdowns"),
        row("none-with-hours", hours(h1=1), status="NoShutdowns"),
        row("emergency", [], status="EmergencyShutdowns"),
        row("waiting", [], status="WaitingForSchedule"),
        row("no-plan", [], status="ScheduleApplies"),
        row("bad-length", [1, 1], status="ScheduleApplies"),
        row("bad-code", hours(h1=4), status="ScheduleApplies"),
        row("both", tomorrow=hours(h2=1), tomorrow_status="ScheduleApplies", tomorrow_possible=hours(h5=3)),
        row("stale", hours(h1=1), today_date="2026-11-05"),
    ]

    days = alerts_days(rows, TODAY)

    assert sorted(days) == [TODAY, TOMORROW]
    assert sorted(days[TODAY].groups) == ["applies", "both", "none", "none-with-hours"]
    assert spans(days[TODAY], "applies") == [(60, 120, False)]
    assert days[TODAY].outages("none") == () and days[TODAY].outages("none-with-hours") == ()
    assert days[TODAY].unknown == {"emergency": EMERGENCY}
    assert sorted(days[TOMORROW].groups) == ["both"]
    assert spans(days[TOMORROW], "both") == [(120, 180, False), (330, 360, True)]
    assert alerts_days([row(status="WaitingForSchedule")], TODAY) == {}


def test_alerts_days_on_real_responses():
    kharkiv_rows = fixture("kharkivska")
    kharkiv = oblenergo_rows(kharkiv_rows)
    assert (len(kharkiv_rows), len(kharkiv)) == (24, 12)
    assert oblenergo_rows(fixture("vinnytska")) == []
    groups = [f"{n}.{k}" for n in range(1, 7) for k in (1, 2)]

    kharkiv_feed = to_yasno(alerts_days(kharkiv, TODAY), groups, TODAY)
    assert kharkiv_feed["1.1"]["today"]["status"] == "ScheduleApplies"
    assert [(s["start"], s["end"], s["type"]) for s in kharkiv_feed["1.1"]["today"]["slots"]] == [
        (0, 180, "Definite"),
        (480, 810, "Definite"),
        (1020, 1440, "Definite"),
    ]
    assert kharkiv_feed["1.1"]["tomorrow"]["status"] == "WaitingForSchedule"

    poltava = to_yasno(alerts_days(oblenergo_rows(fixture("poltavska")), TODAY), ["1.1"], TODAY)
    assert poltava["1.1"]["today"] == {"date": at(TODAY, 0).isoformat(), "status": "EmergencyShutdowns", "slots": []}

    chernivtsi = to_yasno(alerts_days(oblenergo_rows(fixture("chernivecka")), TODAY), ["1", "2", "12"], TODAY)
    assert [(s["start"], s["end"]) for s in chernivtsi["1"]["today"]["slots"]] == [
        (180, 330),
        (510, 720),
        (870, 1080),
        (1230, 1380),
    ]
    assert sorted(chernivtsi) == ["1", "12", "2"]


class FakeResponse:
    def __init__(self, status, headers, body):
        self.status, self.headers = status, headers
        self.content = self
        self._body = body

    async def read(self, size):
        return self._body[:size]

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class FakeSession:
    def __init__(self, response=None, error=None):
        self.response, self.error, self.calls = response, error, []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        if self.error:
            raise self.error
        return self.response


def test_live_get_sends_the_key_and_reads_status_headers_and_body():
    body = json.dumps([{"slug": "kyiv"}]).encode()
    ok = FakeSession(FakeResponse(200, {"X-RateLimit-Daily-Remaining": "1999"}, body))
    limited = FakeSession(FakeResponse(429, {"Retry-After": "90"}, b"slow down"))

    reply = asyncio.run(live_get(ok, "secret-key")("/source-registry/areas/overview"))
    rate_limited = asyncio.run(live_get(limited, "secret-key")("/x"))

    method, url, kwargs = ok.calls[0]
    assert (method, url) == ("GET", "https://alerts.energy/api/v1/source-registry/areas/overview")
    assert kwargs["headers"]["Authorization"] == "Bearer secret-key"
    assert kwargs["allow_redirects"] is False
    assert (reply.status, reply.headers["x-ratelimit-daily-remaining"], reply.body) == (200, "1999", [{"slug": "kyiv"}])
    assert (rate_limited.status, rate_limited.headers["retry-after"], rate_limited.body) == (429, "90", None)


def test_live_get_error_names_the_path_but_not_the_key():
    session = FakeSession(error=aiohttp.ClientConnectionError("connect to Bearer secret-key failed"))

    with pytest.raises(AlertsError) as err:
        asyncio.run(live_get(session, "secret-key")("/source-registry/areas/overview"))

    assert "secret-key" not in str(err.value)
    assert "/source-registry/areas/overview" in str(err.value)
