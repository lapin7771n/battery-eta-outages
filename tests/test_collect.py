import asyncio
import dataclasses
import json
import re
import shutil
from datetime import date, datetime, timedelta
from pathlib import Path

import aiohttp
import pytest
from ua_outages.http import FetchError
from ua_outages.models import DaySchedule, Outage, at
from ua_outages.providers.base import AddressSource, Kind, Provider
from ua_outages.providers.chernihiv import Chernihiv
from ua_outages.providers.dtek import MIRROR_URL, DtekPage, OutageDataUa
from ua_outages.providers.kirovohrad import CHANNELS, Kirovohrad
from ua_outages.providers.yasno import Yasno
from ua_outages.registry import OPERATORS, OPERATORS_BY_ID, Operator

from outage_feed.alerts import ALERTS_AREAS, ALERTS_FALLBACK, ALERTS_ONLY, AlertsError, Reply
from outage_feed.collect import collect, source_id, source_label

NOW = at(date(2026, 11, 6), 12)
OLD_STAMP = "2026-11-06T08:00:00+02:00"
OLD_FILE = '{"1.1":{"today":{"date":"2026-11-06T00:00:00+02:00","status":"ScheduleApplies","slots":[]}}}'


class Failing(Provider):
    async def fetch(self, http, now):
        raise FetchError("down")


class Working(Provider):
    async def fetch(self, http, now):
        today = now.date()
        return {today: DaySchedule(today, groups={"1.1": (Outage(at(today, 8), at(today, 11, 30)),)})}


class AbroadOnly(Working):
    ukraine_only = True


class Empty(Provider):
    async def fetch(self, http, now):
        return {}


class Spy(Working):
    def __init__(self):
        self.fetched = 0

    async def fetch(self, http, now):
        self.fetched += 1
        return await super().fetch(http, now)


class Quiet(Empty):
    pass


class Waiter(Working):
    def __init__(self, event):
        self.event = event

    async def fetch(self, http, now):
        await self.event.wait()
        return await super().fetch(http, now)


class Setter(Waiter):
    async def fetch(self, http, now):
        self.event.set()
        return await Working.fetch(self, http, now)


class Slow(Working):
    async def fetch(self, http, now):
        await asyncio.sleep(5)
        return await super().fetch(http, now)


class PerAddress(AddressSource):
    def valid_address(self, address):
        return True

    async def fetch_address(self, http, now, address):
        raise AssertionError("per-address sources are not polled")


def operator(op_id, *sources, groups=("1.1",)):
    return Operator(op_id, op_id, "region", "https://example.test", sources=sources, groups=groups)


def run(operators, tmp_path, now=NOW, **kwargs):
    previous, out = tmp_path / "previous", tmp_path / "out"
    return asyncio.run(collect(operators, out, previous, now=now, **kwargs)), out


def next_run(operators, tmp_path, minutes, *, start=NOW, **kwargs):
    """Publish the last output as the previous feed, then collect ``minutes`` later."""
    shutil.rmtree(tmp_path / "previous", ignore_errors=True)
    (tmp_path / "out").rename(tmp_path / "previous")
    return run(operators, tmp_path, now=start + timedelta(minutes=minutes), **kwargs)


def head(entry):
    """An ``index.json`` operator entry without its ``sources`` list."""
    return {key: value for key, value in entry.items() if key != "sources"}


def today_slots(path):
    return json.loads(path.read_text())["1.1"]["today"]["slots"]


def test_first_source_with_data_wins_and_failed_sources_keep_previous_file(tmp_path):
    previous = tmp_path / "previous"
    previous.mkdir()
    for op_id in ("down", "fallback", "quiet"):
        (previous / f"{op_id}.json").write_text(OLD_FILE)
    (previous / "index.json").write_text(
        json.dumps({"down": {"updatedAt": OLD_STAMP, "source": "Working", "ok": True}})
    )

    index, out = run(
        [
            operator("down", Failing()),
            operator("fallback", Failing(), Working()),
            operator("empty-first", Empty(), Working()),
            operator("quiet", Failing(), Empty(), Quiet()),
        ],
        tmp_path,
    )

    assert (out / "down.json").read_text() == OLD_FILE
    assert head(index["down"]) == {"updatedAt": OLD_STAMP, "source": "Working", "ok": False}
    assert today_slots(out / "fallback.json") == [{"start": 480, "end": 690, "type": "Definite"}]
    assert head(index["fallback"]) == {"updatedAt": NOW.isoformat(), "source": "Working", "ok": True}
    assert today_slots(out / "empty-first.json") == [{"start": 480, "end": 690, "type": "Definite"}]
    assert index["empty-first"]["source"] == "Working"
    quiet = json.loads((out / "quiet.json").read_text())["1.1"]
    assert [(day["status"], day["slots"]) for day in quiet.values()] == [("WaitingForSchedule", [])] * 2
    assert head(index["quiet"]) == {"updatedAt": NOW.isoformat(), "source": "Empty", "ok": True}


def test_first_run_without_previous_data_writes_no_file(tmp_path):
    index, out = run([operator("new", Failing())], tmp_path)

    assert not (out / "new.json").exists()
    assert head(index["new"]) == {"updatedAt": None, "source": None, "ok": False}


def test_success_writes_yasno_file_and_index(tmp_path):
    index, out = run([operator("a", Working())], tmp_path)

    feed = json.loads((out / "a.json").read_text())
    assert feed["1.1"]["today"]["slots"] == [{"start": 480, "end": 690, "type": "Definite"}]
    assert feed["1.1"]["tomorrow"]["status"] == "WaitingForSchedule"
    assert json.loads((out / "index.json").read_text()) == index
    assert list(index) == ["a"] and head(index["a"]) == {"updatedAt": NOW.isoformat(), "source": "Working", "ok": True}


def test_skips_per_address_operators_and_sources_that_need_a_ukrainian_ip(tmp_path):
    index, out = run(
        [
            operator("addr", PerAddress()),
            operator("none"),
            operator("ua", AbroadOnly()),
            operator("abroad", AbroadOnly(), Working()),
        ],
        tmp_path,
    )

    assert sorted(index) == ["abroad"]
    assert not (out / "addr.json").exists()
    assert index["abroad"]["source"] == "Working"


def test_operators_are_fetched_together_and_a_slow_one_times_out(tmp_path):
    event = asyncio.Event()

    index, _ = run(
        [operator("waiter", Waiter(event)), operator("slow", Slow()), operator("setter", Setter(event))],
        tmp_path,
        timeout=0.5,
    )

    assert list(index) == ["waiter", "slow", "setter"]
    assert index["waiter"]["ok"] and index["setter"]["ok"]
    assert head(index["slow"]) == {"updatedAt": None, "source": None, "ok": False}


ALERTS = Path(__file__).parent / "fixtures" / "alerts"
OVERVIEW = "/source-registry/areas/overview"
SLUGS = {**ALERTS_ONLY, **ALERTS_FALLBACK}


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def time(self):
        return self.now

    async def sleep(self, seconds):
        self.now += seconds


def shutdowns_path(slug):
    return f"/source-registry/areas/{slug}/shutdowns"


class FakeAlerts:
    """The alerts.energy API: serves canned bodies and records each request with its time.

    The overview is a real reply; each reply takes ``delay`` seconds on the fake clock.
    """

    def __init__(self, shutdowns=None, clock=None):
        self.clock = clock or FakeClock()
        overview = json.loads((ALERTS / "overview.json").read_text("utf-8"))
        self.areas = {area["slug"]: area for area in overview}
        self.overview = None
        self.shutdowns = shutdowns or {}
        self.status = {}
        self.headers = {}
        self.errors = set()
        self.delay = 0.0
        self.calls = []

    def paths(self):
        return [path for path, _ in self.calls]

    def reset(self):
        self.calls.clear()

    async def get(self, path):
        self.calls.append((path, self.clock.time()))
        self.clock.now += self.delay
        if path in self.errors:
            raise AlertsError(f"GET {path}: ClientConnectorError")
        if path in self.status:
            return Reply(self.status[path], self.headers.get(path, {}), None)
        headers = self.headers.get(path, {})
        if path == OVERVIEW:
            return Reply(200, headers, list(self.areas.values()) if self.overview is None else self.overview)
        slug = path.split("/")[-2]
        return Reply(200, headers, self.shutdowns.get(slug, [own_row(slug)]))


def own_row(slug, queue="1.1", initiator="_oblenergo"):
    return {
        "initiator": slug.replace("-", "_") + initiator,
        "queue": queue,
        "today": [1] * 4 + [0] * 20,
        "today_possible": [0] * 24,
        "tomorrow": [],
        "today_date": "2026-11-06",
        "tomorrow_date": "2026-11-07",
        "today_status": "ScheduleApplies",
        "tomorrow_status": "WaitingForSchedule",
    }


def real(name):
    return json.loads((ALERTS / f"shutdowns-{name}-oblast.json").read_text("utf-8"))


def alerts_run(operators, tmp_path, api, **kwargs):
    return run(operators, tmp_path, alerts_get=api.get, clock=api.clock.time, sleep=api.clock.sleep, **kwargs)


def test_alerts_feeds_the_listed_operators_and_backs_up_the_rest(tmp_path):
    api = FakeAlerts(
        {
            "kharkivska-oblast": real("kharkivska"),
            "poltavska-oblast": real("poltavska"),
            "zaporizka-oblast": real("zaporizka"),
            "chernivecka-oblast": real("chernivecka"),
            "sumska-oblast": real("vinnytska"),
        }
    )
    spy = Spy()
    operators = [
        operator("kharkiv-oe", Failing(), groups=("1.1",)),
        operator("poltava-oe", Working(), AbroadOnly()),
        operator("zaporizhzhia-oe", spy),
        operator("chernivtsi-oe", Working(), groups=("1", "2")),
        operator("dtek-krem", Failing(), Slow()),
        operator("zhytomyr-oe", Working()),
        operator("dtek-oem", Empty()),
        operator("sumy-oe", Failing()),
        operator("vinnytsia-oe", Failing()),
        operator("kherson-oe"),
    ]
    old = tmp_path / "previous"
    old.mkdir()
    (old / "sumy-oe.json").write_text(OLD_FILE)

    index, out = alerts_run(operators, tmp_path, api, timeout=0.5)

    def slots(op_id, group="1.1", day="today"):
        return json.loads((out / f"{op_id}.json").read_text())[group][day]

    assert spy.fetched == 0
    assert slots("kharkiv-oe")["status"] == "ScheduleApplies"
    assert slots("kharkiv-oe")["slots"][0] == {"start": 0, "end": 180, "type": "Definite"}
    assert slots("poltava-oe")["status"] == "EmergencyShutdowns"
    assert slots("zaporizhzhia-oe")["slots"][0] == {"start": 720, "end": 1020, "type": "Definite"}
    assert slots("chernivtsi-oe", "1")["slots"][0] == {"start": 180, "end": 330, "type": "Definite"}
    assert slots("dtek-krem")["slots"] == [{"start": 0, "end": 240, "type": "Definite"}]
    for op_id in ("kharkiv-oe", "poltava-oe", "zaporizhzhia-oe", "chernivtsi-oe", "dtek-krem"):
        assert head(index[op_id]) == {"updatedAt": NOW.isoformat(), "source": "alerts.energy", "ok": True}
    assert index["zhytomyr-oe"]["source"] == "Working"
    assert index["dtek-oem"]["source"] == "Empty"
    assert slots("vinnytsia-oe")["slots"] == [{"start": 0, "end": 240, "type": "Definite"}]
    assert "kherson-oe" not in index
    assert (out / "sumy-oe.json").read_text() == OLD_FILE
    assert head(index["sumy-oe"]) == {"updatedAt": None, "source": None, "ok": False}
    assert api.paths()[0] == OVERVIEW
    asked = {path.split("/")[-2] for path in api.paths()[1:]}
    assert asked == {
        "kharkivska-oblast",
        "poltavska-oblast",
        "zaporizka-oblast",
        "chernivecka-oblast",
        "kyivska-oblast",
        "sumska-oblast",
        "vinnytska-oblast",
    }
    assert [e["id"] for e in index["zhytomyr-oe"]["sources"]] == ["operator"]

    kwargs = {"alerts_get": api.get, "clock": api.clock.time, "sleep": api.clock.sleep}
    poltava = operator("poltava-oe")
    api.reset()
    failed, out = next_run([poltava, operator("zhytomyr-oe", Failing())], tmp_path, 30, **kwargs)
    assert api.paths() == [OVERVIEW, shutdowns_path("zhytomyrska-oblast")]
    assert failed["zhytomyr-oe"]["source"] == "alerts.energy"
    paid_file = (out / "zhytomyr-oe@alerts.json").read_bytes()
    assert (out / "zhytomyr-oe.json").read_bytes() == paid_file

    api.reset()
    api.areas["zhytomyrska-oblast"]["mode"] = "emergency"
    recovered, out = next_run([poltava, operator("zhytomyr-oe", Working())], tmp_path, 60, **kwargs)
    assert api.paths() == [OVERVIEW]
    assert (out / "zhytomyr-oe@alerts.json").read_bytes() == paid_file
    assert recovered["zhytomyr-oe"]["sources"][-1] == failed["zhytomyr-oe"]["sources"][-1]
    assert recovered["zhytomyr-oe"]["source"] == "Working"
    assert today_slots(out / "zhytomyr-oe.json") == [{"start": 480, "end": 690, "type": "Definite"}]


def all_alerts_operators(failing=ALERTS_FALLBACK):
    return [operator(op_id) for op_id in ALERTS_ONLY] + [
        operator(op_id, Failing() if op_id in failing else Working())
        for op_id in ALERTS_AREAS if op_id not in ALERTS_ONLY
    ]


def test_alerts_budget(tmp_path):
    api = FakeAlerts()
    operators = all_alerts_operators(failing={"dtek-krem"})
    kwargs = {"alerts_get": api.get, "clock": api.clock.time, "sleep": api.clock.sleep}

    first, _ = alerts_run(operators, tmp_path, api)
    assert len(api.calls) == 6
    assert [path for path, _ in api.calls][0] == OVERVIEW
    times = [at for _, at in api.calls]
    assert all(b - a >= 0.5 - 1e-9 for a, b in zip(times, times[1:], strict=False))
    assert first["alertsCheckedAt"] == NOW.isoformat()
    assert set(first["alertsAreas"]) == set(ALERTS_ONLY) | {"dtek-krem"}
    assert first["alertsAreas"]["poltava-oe"] == {
        "change": ["2026-11-06T10:49:29.000Z", "regional-emergency", True, False, "regional"],
        "fetchedAt": NOW.isoformat(),
    }

    api.reset()
    soon, _ = next_run(operators, tmp_path, 24, **kwargs)
    assert api.calls == []
    assert soon["alertsCheckedAt"] == NOW.isoformat()
    assert all(soon[op_id] == first[op_id] for op_id in first["alertsAreas"])
    assert soon["poltava-oe"]["sources"] == first["poltava-oe"]["sources"]

    unchanged, _ = next_run(operators, tmp_path, 26, **kwargs)
    assert api.paths() == [OVERVIEW]
    assert unchanged["alertsCheckedAt"] == (NOW + timedelta(minutes=26)).isoformat()
    assert head(unchanged["poltava-oe"]) == {
        "updatedAt": (NOW + timedelta(minutes=26)).isoformat(),
        "source": "alerts.energy",
        "ok": True,
    }
    assert unchanged["poltava-oe"]["sources"] == [
        {
            "id": "alerts",
            "name": "alerts.energy",
            "ok": True,
            "updatedAt": (NOW + timedelta(minutes=26)).isoformat(),
            "queues": first["poltava-oe"]["sources"][0]["queues"],
        }
    ]
    kept = tmp_path / "out"
    assert (kept / "poltava-oe.json").read_bytes() == (kept / "poltava-oe@alerts.json").read_bytes()

    api.reset()
    for area in api.areas.values():
        area["updated"] = "2026-11-06T11:00:00.000Z"
    next_run(operators, tmp_path, 80, **kwargs)
    assert len(api.calls) == 6
    times = [at for _, at in api.calls]
    assert all(b - a >= 0.5 - 1e-9 for a, b in zip(times, times[1:], strict=False))


def test_alerts_refetches_what_changed_is_six_hours_old_or_lacks_a_file_for_today(tmp_path):
    api = FakeAlerts()
    operators = all_alerts_operators(failing={"dtek-krem"})
    kwargs = {"alerts_get": api.get, "clock": api.clock.time, "sleep": api.clock.sleep}
    alerts_run(operators, tmp_path, api)

    def asked_at(minutes):
        api.reset()
        next_run(operators, tmp_path, minutes, **kwargs)
        return api.paths()

    api.areas["poltavska-oblast"]["mode"] = "scheduled"
    assert asked_at(30) == [OVERVIEW, shutdowns_path("poltavska-oblast")]

    (tmp_path / "out" / "poltava-oe@alerts.json").unlink()
    assert asked_at(60) == [OVERVIEW, shutdowns_path("poltavska-oblast")]

    (tmp_path / "out" / "zaporizhzhia-oe@alerts.json").write_text(OLD_FILE.replace("2026-11-06", "2026-11-05"))
    assert asked_at(90) == [OVERVIEW, shutdowns_path("zaporizka-oblast")]

    (tmp_path / "out" / "dtek-krem@alerts.json").unlink()
    assert asked_at(116) == [OVERVIEW, shutdowns_path("kyivska-oblast")]

    assert asked_at(330) == [OVERVIEW]
    assert asked_at(360) == [OVERVIEW, shutdowns_path("chernivecka-oblast"), shutdowns_path("vinnytska-oblast")]


def test_alerts_pass_stops_on_429_and_on_a_low_daily_quota(tmp_path, capsys):
    zaporizhzhia = shutdowns_path("zaporizka-oblast")
    order = list(ALERTS_ONLY.values()) + list(ALERTS_FALLBACK.values())
    until_zaporizhzhia = [OVERVIEW] + [shutdowns_path(slug) for slug in order[:2]]
    assert until_zaporizhzhia[-1] == zaporizhzhia

    for name in ("429", "quota"):
        directory = tmp_path / name
        directory.mkdir()
        api = FakeAlerts()
        api.delay = 45
        if name == "429":
            api.status[zaporizhzhia], api.headers[zaporizhzhia] = 429, {"retry-after": "3600"}
        else:
            api.headers[OVERVIEW] = {"x-ratelimit-daily-remaining": "30"}
            api.headers[zaporizhzhia] = {"x-ratelimit-daily-remaining": "29"}
        operators = all_alerts_operators(failing=())
        kwargs = {"alerts_get": api.get, "clock": api.clock.time, "sleep": api.clock.sleep}

        index, out = run(operators, directory, **kwargs)

        assert api.paths() == until_zaporizhzhia, name
        left = "29" if name == "quota" else "unknown"
        assert f"alerts.energy: pass sent 3 requests, {left} left today" in capsys.readouterr().err, name
        assert index["poltava-oe"]["ok"] is True, name
        assert index["zaporizhzhia-oe"]["ok"] is (name == "quota"), name
        assert head(index["chernivtsi-oe"]) == {"updatedAt": None, "source": None, "ok": False}, name
        assert not (out / "chernivtsi-oe.json").exists(), name
        assert ("alertsRetryAfter" in index) == (name == "429"), name
        if name == "429":
            replied_at = NOW + timedelta(seconds=3 * 45 + 2 * 0.5)
            retry_after = datetime.fromisoformat(index["alertsRetryAfter"])
            assert abs(retry_after - (replied_at + timedelta(seconds=3600))) < timedelta(seconds=1)

        api.reset()
        api.status.clear()
        api.delay = 0
        next_run(operators, directory, 40, **kwargs)
        assert (len(api.calls) == 0) == (name == "429"), name

        api.reset()
        next_run(operators, directory, 70, **kwargs)
        assert api.paths()[0] == OVERVIEW, name
        assert (len(api.calls) > 1) == (name == "429"), name


def test_alerts_network_error_fails_only_that_area(tmp_path):
    api = FakeAlerts()
    api.errors.add(shutdowns_path("poltavska-oblast"))
    previous = tmp_path / "previous"
    previous.mkdir()
    (previous / "poltava-oe.json").write_text(OLD_FILE)
    (previous / "poltava-oe@alerts.json").write_text(OLD_FILE)
    old_source = {"id": "alerts", "name": "alerts.energy", "ok": True, "updatedAt": OLD_STAMP, "queues": ["1.1"]}
    old_entry = {"updatedAt": OLD_STAMP, "source": "alerts.energy", "ok": True, "sources": [old_source]}
    (previous / "index.json").write_text(json.dumps({"poltava-oe": old_entry}))

    index, out = alerts_run(all_alerts_operators(failing={"sumy-oe"}), tmp_path, api)

    assert head(index["poltava-oe"]) == {"updatedAt": OLD_STAMP, "source": "alerts.energy", "ok": False}
    assert (out / "poltava-oe.json").read_text() == (out / "poltava-oe@alerts.json").read_text() == OLD_FILE
    assert index["poltava-oe"]["sources"] == [{**old_source, "ok": False}]
    assert len(api.calls) == 6
    others = [op_id for op_id in SLUGS if op_id != "poltava-oe"]
    assert all(index[op_id]["ok"] and (out / f"{op_id}.json").exists() for op_id in others)


def test_alerts_overview_it_cannot_read_is_logged(tmp_path, capsys):
    api = FakeAlerts()
    del api.areas["sumska-oblast"]["updated"]

    index, _ = alerts_run(all_alerts_operators(), tmp_path / "no-time", api)

    assert "sumy-oe: alerts.energy overview has no updated time for sumska-oblast" in capsys.readouterr().err
    assert shutdowns_path("sumska-oblast") not in api.paths()
    assert index["sumy-oe"]["ok"] is False and index["poltava-oe"]["ok"] is True

    api.reset()
    api.overview = {"areas": list(api.areas.values())}

    index, _ = alerts_run(all_alerts_operators(), tmp_path / "wrapped", api)

    assert "alerts.energy: overview is dict, not a list" in capsys.readouterr().err
    assert api.paths() == [OVERVIEW]
    assert not any(index[op_id]["ok"] for op_id in SLUGS)


def test_alerts_pass_keeps_what_it_fetched_before_the_deadline(tmp_path):
    api = FakeAlerts()
    api.delay = 50

    class SlowFree(Working):
        async def fetch(self, http, now):
            await api.clock.sleep(150)
            return await super().fetch(http, now)

    operators = [operator("free", SlowFree()), *all_alerts_operators()]
    index, out = alerts_run(operators, tmp_path, api, alerts_timeout=120)

    assert index["free"]["ok"] and api.calls[0][1] == 1150

    assert api.paths() == [OVERVIEW, shutdowns_path("poltavska-oblast"), shutdowns_path("zaporizka-oblast")]
    assert index["poltava-oe"]["ok"] is True and index["zaporizhzhia-oe"]["ok"] is True
    assert today_slots(out / "poltava-oe.json") == [{"start": 0, "end": 240, "type": "Definite"}]
    assert sorted(index["alertsAreas"]) == ["poltava-oe", "zaporizhzhia-oe"]
    assert index["chernivtsi-oe"]["ok"] is False and not (out / "chernivtsi-oe.json").exists()


def test_alerts_without_a_key_is_skipped_and_the_run_still_succeeds(tmp_path, monkeypatch):
    def no_network(*args):
        raise AssertionError("alerts.energy must not be called without a key")

    monkeypatch.delenv("ALERTS_ENERGY_KEY", raising=False)
    monkeypatch.setattr("outage_feed.collect.live_get", no_network)
    previous = tmp_path / "previous"
    previous.mkdir()
    (previous / "poltava-oe.json").write_text(OLD_FILE)

    index, out = run([operator("poltava-oe"), operator("sumy-oe", Failing()), operator("free", Working())], tmp_path)

    assert (out / "poltava-oe.json").read_text() == OLD_FILE
    assert index["poltava-oe"]["ok"] is False and index["sumy-oe"]["ok"] is False
    assert index["free"]["ok"] is True
    assert "alertsCheckedAt" not in index


def test_alerts_pass_uses_the_key_from_the_environment(tmp_path, monkeypatch):
    api = FakeAlerts()
    keys = []

    def fake_live_get(session, key):
        keys.append(key)
        return api.get

    monkeypatch.setenv("ALERTS_ENERGY_KEY", "secret-key")
    monkeypatch.setattr("outage_feed.collect.live_get", fake_live_get)

    index, _ = run([operator("poltava-oe")], tmp_path)

    assert keys == ["secret-key"] and index["poltava-oe"]["ok"] is True


def posing(base, behaviour, *args):
    """A ``base`` source (``Yasno``, ``DtekPage``...) that fetches the way ``behaviour`` does."""

    class Posed(base):
        async def fetch(self, http, now):
            return await behaviour.fetch(http, now)

    return Posed(*args)


def registry_operator(op_id, *sources):
    return dataclasses.replace(OPERATORS_BY_ID[op_id], sources=sources)


def feed_file(out, name):
    return json.loads((out / f"{name}.json").read_text())


def dnem_sources(yasno=None, site=None, mirror=None):
    return registry_operator(
        "dtek-dnem",
        posing(Yasno, yasno or Working(), 3, 301),
        posing(DtekPage, site or Working(), "dnem"),
        posing(OutageDataUa, mirror or Empty(), "dnipro"),
    )


def fixture_rows(name):
    return json.loads((ALERTS / f"shutdowns-{name}.json").read_text("utf-8"))


def dnipro_api():
    return FakeAlerts({"dnipropetrovska-oblast": fixture_rows("dnipropetrovska-oblast")})


def test_source_ids_and_labels_of_the_registry():
    ids = {}
    for op in OPERATORS:
        polled = [s for s in op.sources if not s.ukraine_only]
        ids[op.id] = [(source_id(s), source_label(s)) for s in polled]
        assert len({sid for sid, _ in ids[op.id]}) == len(polled), op.id
        assert all(re.fullmatch(r"[a-z0-9-]{2,40}", sid) for sid, _ in ids[op.id]), op.id

    assert ids["dtek-kem"] == [("yasno", "YASNO"), ("dtek-mirror", "Копія ДТЕК (outage-data-ua)")]
    assert ids["dtek-krem"] == [("dtek-mirror", "Копія ДТЕК (outage-data-ua)")]
    assert [sid for sid, _ in ids["dtek-dnem"]] == ["yasno", "dtek-site", "dtek-mirror"]
    assert [sid for sid, _ in ids["cek"]] == ["yasno", "dtek-site", "dtek-mirror"]
    assert ids["dtek-dnem"][1] == ("dtek-site", "Сайт ДТЕК")
    assert ids["kirovohrad-oe"] == [
        ("telegram-svitlokropyvnytskyimisto", "Telegram: SvitloKropyvnytskyiMisto"),
        ("telegram-svitlooleksandriyskohoraionu", "Telegram: SvitloOleksandriyskohoRaionu"),
    ]
    assert ids["chernihiv-oe"] == [("telegram-chernigivoblenergo", "Telegram: chernigivoblenergo")]
    assert ids["zhytomyr-oe"] == [("operator", "Сайт обленерго")]
    assert ids["sumy-oe"] == [("operator", "Сайт обленерго")]
    assert ids["kharkiv-oe"] == [("telegram-kharkivenergy", "Telegram: kharkivenergy")]
    assert ids["vinnytsia-oe"] == []


class Many(Provider):
    async def fetch(self, http, now):
        today = now.date()
        slot = (Outage(at(today, 8), at(today, 9)),)
        return {today: DaySchedule(today, groups={"1.1": slot, "2.1": slot, "10.1": slot})}


def test_every_source_of_an_operator_gets_its_own_file_and_index_entry(tmp_path):
    op = dataclasses.replace(dnem_sources(yasno=Many()), groups=("10.1", "2.1", "1.1"))

    index, out = alerts_run([op], tmp_path, dnipro_api())

    yasno = feed_file(out, "dtek-dnem@yasno")
    assert yasno["10.1"]["today"]["slots"] == [{"start": 480, "end": 540, "type": "Definite"}]
    assert sorted(feed_file(out, "dtek-dnem@dtek-site")) == ["1.1"]
    waiting = feed_file(out, "dtek-dnem@dtek-mirror")
    assert [day["status"] for day in waiting["1.1"].values()] == ["WaitingForSchedule"] * 2
    assert not (out / "dtek-dnem@alerts.json").exists()
    stamp = NOW.isoformat()
    assert index["dtek-dnem"]["sources"] == [
        {"id": "yasno", "name": "YASNO", "ok": True, "updatedAt": stamp, "queues": ["1.1", "2.1", "10.1"]},
        {"id": "dtek-site", "name": "Сайт ДТЕК", "ok": True, "updatedAt": stamp, "queues": ["1.1"]},
        {"id": "dtek-mirror", "name": "Копія ДТЕК (outage-data-ua)", "ok": True, "updatedAt": stamp, "queues": ["1.1", "2.1", "10.1"]},
    ]
    assert json.loads((out / "index.json").read_text())["dtek-dnem"] == index["dtek-dnem"]
    assert feed_file(out, "dtek-dnem") == yasno
    assert head(index["dtek-dnem"]) == {"updatedAt": stamp, "source": "YASNO 3/301", "ok": True}


def test_a_failed_source_keeps_its_previous_file_and_the_others_are_written(tmp_path):
    previous = tmp_path / "previous"
    previous.mkdir()
    (previous / "dtek-dnem@dtek-site.json").write_text(OLD_FILE)
    old_sources = [{"id": "dtek-site", "name": "Сайт ДТЕК", "ok": True, "updatedAt": OLD_STAMP, "queues": ["1.1"]}]
    (previous / "index.json").write_text(json.dumps({"dtek-dnem": {"sources": old_sources}}))

    index, out = alerts_run([dnem_sources(site=Failing(), mirror=Failing())], tmp_path, dnipro_api())

    assert (out / "dtek-dnem@dtek-site.json").read_text() == OLD_FILE
    assert not (out / "dtek-dnem@dtek-mirror.json").exists()
    assert [(e["id"], e["ok"]) for e in index["dtek-dnem"]["sources"]] == [
        ("yasno", True),
        ("dtek-site", False),
    ]
    assert index["dtek-dnem"]["sources"][1] == {**old_sources[0], "ok": False}
    assert today_slots(out / "dtek-dnem@yasno.json") == [{"start": 480, "end": 690, "type": "Definite"}]


def test_sources_of_one_operator_are_fetched_together_each_with_its_own_timeout(tmp_path):
    event = asyncio.Event()
    previous = tmp_path / "previous"
    previous.mkdir()
    (previous / "dtek-dnem@dtek-mirror.json").write_text(OLD_FILE)
    op = dnem_sources(yasno=Waiter(event), site=Setter(event), mirror=Slow())

    index, out = run([op], tmp_path, timeout=0.5)

    assert [(e["id"], e["ok"]) for e in index["dtek-dnem"]["sources"]] == [
        ("yasno", True),
        ("dtek-site", True),
        ("dtek-mirror", False),
    ]
    assert (out / "dtek-dnem@dtek-mirror.json").read_text() == OLD_FILE


def test_the_two_telegram_channels_of_kirovohrad_get_their_own_files(tmp_path):
    op = registry_operator("kirovohrad-oe", *(posing(Kirovohrad, Working(), channel) for channel in CHANNELS))

    index, out = alerts_run([op], tmp_path, FakeAlerts())

    assert [(e["id"], e["name"]) for e in index["kirovohrad-oe"]["sources"]] == [
        ("telegram-svitlokropyvnytskyimisto", "Telegram: SvitloKropyvnytskyiMisto"),
        ("telegram-svitlooleksandriyskohoraionu", "Telegram: SvitloOleksandriyskohoRaionu"),
    ]
    assert feed_file(out, "kirovohrad-oe@telegram-svitlokropyvnytskyimisto") == feed_file(
        out, "kirovohrad-oe@telegram-svitlooleksandriyskohoraionu"
    )
    assert head(index["kirovohrad-oe"])["source"] == "Telegram SvitloKropyvnytskyiMisto"


def test_vinnytsia_and_chernihiv_are_filled_from_alerts_when_they_have_no_own_data(tmp_path):
    api = FakeAlerts(
        {
            "vinnytska-oblast": fixture_rows("vinnytska-oblast-with-rows"),
            "chernigivska-oblast": fixture_rows("chernigivska-oblast"),
        }
    )
    operators = [
        OPERATORS_BY_ID["vinnytsia-oe"],
        registry_operator("chernihiv-oe", posing(Chernihiv, Empty())),
    ]

    index, out = alerts_run(operators, tmp_path, api)

    vinnytsia = feed_file(out, "vinnytsia-oe")
    assert sorted(vinnytsia) == ["1.1", "1.2", "2.1", "2.2", "4.1", "5.1", "5.2", "6.1", "6.2"]
    assert vinnytsia["1.1"]["today"]["status"] == "ScheduleApplies"
    assert head(index["vinnytsia-oe"]) == {"updatedAt": NOW.isoformat(), "source": "alerts.energy", "ok": True}
    assert [e["queues"] for e in index["vinnytsia-oe"]["sources"]] == [sorted(vinnytsia, key=float)]
    assert feed_file(out, "chernihiv-oe") == feed_file(out, "chernihiv-oe@alerts")
    assert feed_file(out, "chernihiv-oe")["1.1"]["today"]["status"] == "EmergencyShutdowns"
    assert head(index["chernihiv-oe"])["source"] == "alerts.energy"
    assert [e["id"] for e in index["chernihiv-oe"]["sources"]] == ["telegram-chernigivoblenergo", "alerts"]


def test_chernihiv_keeps_its_own_telegram_schedule_when_it_has_queues(tmp_path):
    api = FakeAlerts({"chernigivska-oblast": fixture_rows("chernigivska-oblast")})

    index, out = alerts_run([registry_operator("chernihiv-oe", posing(Chernihiv, Working()))], tmp_path, api)

    assert head(index["chernihiv-oe"])["source"] != "alerts.energy"
    assert feed_file(out, "chernihiv-oe")["1.1"]["today"]["slots"] == [{"start": 480, "end": 690, "type": "Definite"}]
    assert not (out / "chernihiv-oe@alerts.json").exists()
    assert api.calls == []


def test_excluded_operators_keep_their_previous_alerts_source(tmp_path):
    api = FakeAlerts()
    previous = tmp_path / "previous"
    previous.mkdir()
    old_index = {}
    operators = []
    for op_id, source, old_ok in (
        ("dtek-kem", Working(), False),
        ("dtek-dnem", Failing(), True),
        ("cek", Failing(), False),
    ):
        operators.append(registry_operator(op_id, source))
        (previous / f"{op_id}@alerts.json").write_text(OLD_FILE)
        entry = {"id": "alerts", "name": "alerts.energy", "ok": old_ok, "updatedAt": OLD_STAMP, "queues": ["1.1"]}
        old_index[op_id] = {"sources": [entry]}
    (previous / "index.json").write_text(json.dumps(old_index))

    index, out = alerts_run(operators, tmp_path, api)

    assert api.calls == []
    for op in operators:
        assert (out / f"{op.id}@alerts.json").read_text() == OLD_FILE
        assert index[op.id]["sources"][-1] == old_index[op.id]["sources"][0]
    assert today_slots(out / "dtek-kem.json") == [{"start": 480, "end": 690, "type": "Definite"}]
    for op_id in ("dtek-dnem", "cek"):
        assert not index[op_id]["ok"] and not (out / f"{op_id}.json").exists()


def test_daily_alerts_budget_with_two_failing_fallbacks(tmp_path):
    assert sorted(ALERTS_AREAS) == sorted(
        [
            "dtek-kem",
            "dtek-krem",
            "dtek-dnem",
            "cek",
            "dtek-oem",
            "zhytomyr-oe",
            "mykolaiv-oe",
            "cherkasy-oe",
            "sumy-oe",
            "kirovohrad-oe",
            "kharkiv-oe",
            "zaporizhzhia-oe",
            "poltava-oe",
            "chernivtsi-oe",
            "vinnytsia-oe",
            "chernihiv-oe",
        ]
    )
    api = FakeAlerts()
    failing = {"kharkiv-oe", "sumy-oe"}
    operators = all_alerts_operators(failing)
    wanted = set(ALERTS_ONLY) | failing
    start = at(NOW.date(), 0)
    kwargs = {"alerts_get": api.get, "clock": api.clock.time, "sleep": api.clock.sleep}
    hour = 0
    expected_slot = {"start": 0, "end": 240, "type": "Definite"}
    for step in range(48):
        if step % 6 == 1:
            hour += 1
            expected_slot = {"start": hour * 60, "end": (hour + 1) * 60, "type": "Definite"}
            for slug, area in api.areas.items():
                area["updated"] = (start + timedelta(minutes=30 * step)).isoformat()
                api.shutdowns[slug] = [{**own_row(slug), "today": [0] * hour + [1] + [0] * (23 - hour)}]
        if step == 0:
            index, out = run(operators, tmp_path, now=start, **kwargs)
        else:
            index, out = next_run(operators, tmp_path, 30 * step, start=start, **kwargs)
        for op in operators:
            assert index[op.id]["ok"]
            assert (index[op.id]["source"] == "alerts.energy") == (op.id in wanted)
            if op.id in wanted:
                assert today_slots(out / f"{op.id}.json") == [expected_slot]

    assert api.paths().count(OVERVIEW) == 48
    assert len(api.calls) <= 220


def test_an_alerts_pass_stops_at_the_request_cap(tmp_path):
    api = FakeAlerts()
    operators = sorted(all_alerts_operators(), key=lambda op: op.id in ALERTS_ONLY)

    index, out = alerts_run(operators, tmp_path, api)

    fetched = list(ALERTS_ONLY) + ["kharkiv-oe", "dtek-krem", "zhytomyr-oe"]
    assert api.paths() == [OVERVIEW] + [shutdowns_path(SLUGS[op_id]) for op_id in fetched]
    for op_id in SLUGS:
        assert index[op_id]["ok"] == (op_id in fetched)
        assert (out / f"{op_id}@alerts.json").exists() == (op_id in fetched)


class Channel(Working):
    kind = Kind.TELEGRAM

    def __init__(self, channel):
        self.channel = channel


def test_a_source_id_the_app_rejects_or_repeats_gets_no_file(tmp_path, capsys):
    longest, too_long = "y" * 31, "x" * 32
    op = operator(
        "op", Channel("svitlo_Misto"), Channel(longest), Channel(too_long), Channel("bad/name"), Working(), Working()
    )

    index, out = run([op], tmp_path)

    assert [(e["id"], e["name"]) for e in index["op"]["sources"]] == [
        ("telegram-svitlo-misto", "Telegram: svitlo_Misto"),
        (f"telegram-{longest}", f"Telegram: {longest}"),
        ("telegram", "Telegram"),
        ("operator", "Сайт обленерго"),
    ]
    published = ["op.json", "op@telegram-svitlo-misto.json", f"op@telegram-{longest}.json", "op@telegram.json"]
    assert sorted(path.name for path in out.iterdir()) == sorted([*published, "op@operator.json", "index.json"])
    err = capsys.readouterr().err
    assert f"'telegram-{too_long}', not published" in err and "'operator', not published" in err


class FakeSession:
    """An aiohttp session that serves ``pages`` by URL and records each request."""

    def __init__(self, pages):
        self.pages = pages
        self.asked = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        pass

    def request(self, method, url, **kwargs):
        self.asked.append(str(url))
        if self.pages[str(url)] is None:
            raise aiohttp.ClientConnectionError("down")
        return FakePage(str(url), self.pages[str(url)])


class FakePage:
    status = 200
    charset = "utf-8"

    def __init__(self, url, body):
        self.url = url
        self.body = body.encode()
        self.headers = {}
        self.content = self

    def __await__(self):
        # aiohttp's ``request`` is awaitable and an async context manager at once.
        return asyncio.sleep(0, self).__await__()

    async def __aenter__(self):
        await asyncio.sleep(0)
        return self

    async def __aexit__(self, *exc):
        pass

    async def iter_chunked(self, size):
        await asyncio.sleep(0)
        yield self.body


def test_the_dtek_page_and_mirror_are_fetched_once_for_dnem_and_cek(tmp_path, monkeypatch):
    def off_in_hour(hour):
        return {str(h): "no" if h == hour else "yes" for h in range(1, 25)}

    stamp = int(at(NOW.date(), 0).timestamp())
    fact = {"data": {str(stamp): {"GPV1.1": off_in_hour(1), "GPV1001.1": off_in_hour(2)}}, "today": stamp}
    dtek_page = "https://www.dtek-dnem.com.ua/ua/shutdowns"
    mirror = MIRROR_URL.format(region="dnipro")
    session = FakeSession(
        {
            dtek_page: f"<script>DisconSchedule.fact = {json.dumps(fact)}</script>",
            mirror: json.dumps({"lastUpdated": NOW.isoformat(), "fact": fact}),
        }
    )
    monkeypatch.setattr("outage_feed.collect.new_session", lambda: session)
    operators = [
        registry_operator(op_id, posing(Yasno, Working(), 3, 301), *OPERATORS_BY_ID[op_id].sources[1:])
        for op_id in ("dtek-dnem", "cek")
    ]

    _, out = alerts_run(operators, tmp_path, dnipro_api())

    assert sorted(session.asked) == sorted([dtek_page, mirror])
    for name in ("dtek-site", "dtek-mirror"):
        assert today_slots(out / f"dtek-dnem@{name}.json") == [{"start": 0, "end": 60, "type": "Definite"}]
        assert today_slots(out / f"cek@{name}.json") == [{"start": 60, "end": 120, "type": "Definite"}]


KHARKIV_CHANNEL = "https://t.me/s/kharkivenergy"
KHARKIV_NOW = at(date(2026, 11, 24), 12)


def kharkiv_run(tmp_path, monkeypatch, page):
    session = FakeSession({KHARKIV_CHANNEL: page})
    monkeypatch.setattr("outage_feed.collect.new_session", lambda: session)
    api = FakeAlerts({"kharkivska-oblast": real("kharkivska")})
    index, out = alerts_run([OPERATORS_BY_ID["kharkiv-oe"]], tmp_path, api, now=KHARKIV_NOW)
    assert session.asked == [KHARKIV_CHANNEL]
    return index, out, api


def test_kharkiv_reads_its_telegram_channel_and_leaves_alerts_energy_alone(tmp_path, monkeypatch):
    page = (ALERTS.parent / "kharkivenergy-channel.html").read_text("utf-8")

    index, out, api = kharkiv_run(tmp_path, monkeypatch, page)

    assert api.calls == []
    assert head(index["kharkiv-oe"]) == {
        "updatedAt": KHARKIV_NOW.isoformat(),
        "source": "Telegram kharkivenergy",
        "ok": True,
    }
    assert [e["id"] for e in index["kharkiv-oe"]["sources"]] == ["telegram-kharkivenergy"]
    feed = feed_file(out, "kharkiv-oe")
    assert feed["1.1"]["today"]["slots"] == [
        {"start": 0, "end": 90, "type": "Definite"},
        {"start": 510, "end": 930, "type": "Definite"},
        {"start": 1140, "end": 1350, "type": "Definite"},
    ]
    assert feed["1.1"]["tomorrow"]["slots"] == [
        {"start": 360, "end": 540, "type": "Definite"},
        {"start": 960, "end": 1170, "type": "Definite"},
    ]
    assert feed == feed_file(out, "kharkiv-oe@telegram-kharkivenergy")


def test_kharkiv_falls_back_to_alerts_energy_when_its_telegram_channel_fails(tmp_path, monkeypatch):
    index, out, api = kharkiv_run(tmp_path, monkeypatch, None)

    assert api.paths() == [OVERVIEW, shutdowns_path("kharkivska-oblast")]
    assert head(index["kharkiv-oe"]) == {
        "updatedAt": KHARKIV_NOW.isoformat(),
        "source": "alerts.energy",
        "ok": True,
    }
    assert [e["id"] for e in index["kharkiv-oe"]["sources"]] == ["alerts"]
    assert feed_file(out, "kharkiv-oe") == feed_file(out, "kharkiv-oe@alerts")
