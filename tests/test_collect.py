import asyncio
import json
import shutil
from datetime import date, datetime, timedelta
from pathlib import Path

from ua_outages.http import FetchError
from ua_outages.models import DaySchedule, Outage, at
from ua_outages.providers.base import AddressSource, Provider
from ua_outages.registry import Operator

from outage_feed.alerts import ALERTS_FALLBACK, ALERTS_ONLY, AlertsError, Reply
from outage_feed.collect import collect

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


def next_run(operators, tmp_path, minutes, **kwargs):
    """Publish the last output as the previous feed, then collect ``minutes`` later."""
    shutil.rmtree(tmp_path / "previous", ignore_errors=True)
    (tmp_path / "out").rename(tmp_path / "previous")
    return run(operators, tmp_path, now=NOW + timedelta(minutes=minutes), **kwargs)


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
    assert index["down"] == {"updatedAt": OLD_STAMP, "source": "Working", "ok": False}
    assert today_slots(out / "fallback.json") == [{"start": 480, "end": 690, "type": "Definite"}]
    assert index["fallback"] == {"updatedAt": NOW.isoformat(), "source": "Working", "ok": True}
    assert today_slots(out / "empty-first.json") == [{"start": 480, "end": 690, "type": "Definite"}]
    assert index["empty-first"]["source"] == "Working"
    quiet = json.loads((out / "quiet.json").read_text())["1.1"]
    assert [(day["status"], day["slots"]) for day in quiet.values()] == [("WaitingForSchedule", [])] * 2
    assert index["quiet"] == {"updatedAt": NOW.isoformat(), "source": "Empty", "ok": True}


def test_first_run_without_previous_data_writes_no_file(tmp_path):
    index, out = run([operator("new", Failing())], tmp_path)

    assert not (out / "new.json").exists()
    assert index["new"] == {"updatedAt": None, "source": None, "ok": False}


def test_success_writes_yasno_file_and_index(tmp_path):
    index, out = run([operator("a", Working())], tmp_path)

    feed = json.loads((out / "a.json").read_text())
    assert feed["1.1"]["today"]["slots"] == [{"start": 480, "end": 690, "type": "Definite"}]
    assert feed["1.1"]["tomorrow"]["status"] == "WaitingForSchedule"
    assert json.loads((out / "index.json").read_text()) == index == {
        "a": {"updatedAt": NOW.isoformat(), "source": "Working", "ok": True}
    }


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
    assert index["slow"] == {"updatedAt": None, "source": None, "ok": False}


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


def fallback_operators():
    return [operator(op_id, Failing()) for op_id in ALERTS_FALLBACK]


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
        operator("kharkiv-oe", spy, groups=("1.1",)),
        operator("poltava-oe", Working(), AbroadOnly()),
        operator("zaporizhzhia-oe"),
        operator("chernivtsi-oe", Working(), groups=("1", "2")),
        operator("dtek-krem", Failing()),
        operator("zhytomyr-oe", Working()),
        operator("dtek-oem", Empty()),
        operator("sumy-oe", Failing()),
        operator("cherkasy-oe", Failing(), Slow()),
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
        assert index[op_id] == {"updatedAt": NOW.isoformat(), "source": "alerts.energy", "ok": True}
    assert index["zhytomyr-oe"]["source"] == "Working"
    assert index["dtek-oem"]["source"] == "Empty"
    assert not (out / "vinnytsia-oe.json").exists() and "kherson-oe" not in index
    assert (out / "sumy-oe.json").read_text() == OLD_FILE
    assert index["sumy-oe"] == {"updatedAt": None, "source": None, "ok": False}
    assert index["cherkasy-oe"]["source"] == "alerts.energy"
    assert api.paths()[0] == OVERVIEW
    asked = {path.split("/")[-2] for path in api.paths()[1:]}
    assert asked == {
        "kharkivska-oblast",
        "poltavska-oblast",
        "zaporizka-oblast",
        "chernivecka-oblast",
        "kyivska-oblast",
        "sumska-oblast",
        "cherkaska-oblast",
    }


def all_alerts_operators():
    return [operator(op_id) for op_id in ALERTS_ONLY] + fallback_operators()


def test_alerts_budget(tmp_path):
    api = FakeAlerts()
    operators = all_alerts_operators()
    kwargs = {"alerts_get": api.get, "clock": api.clock.time, "sleep": api.clock.sleep}

    first, _ = alerts_run(operators, tmp_path, api)
    assert len(api.calls) == 1 + len(SLUGS) == 12
    assert [path for path, _ in api.calls][0] == OVERVIEW
    times = [at for _, at in api.calls]
    assert all(b - a >= 0.2 - 1e-9 for a, b in zip(times, times[1:], strict=False))
    assert first["alertsCheckedAt"] == NOW.isoformat()
    assert sorted(first["alertsAreas"]) == sorted(SLUGS.values())
    assert first["alertsAreas"]["poltavska-oblast"] == {
        "change": ["2026-11-06T10:49:29.000Z", "regional-emergency", True, False, "regional"],
        "fetchedAt": NOW.isoformat(),
    }

    api.reset()
    soon, _ = next_run(operators, tmp_path, 24, **kwargs)
    assert api.calls == []
    assert soon["alertsCheckedAt"] == NOW.isoformat()
    assert all(soon[op_id] == first[op_id] for op_id in SLUGS)

    unchanged, _ = next_run(operators, tmp_path, 26, **kwargs)
    assert api.paths() == [OVERVIEW]
    assert unchanged["alertsCheckedAt"] == (NOW + timedelta(minutes=26)).isoformat()
    assert unchanged["kharkiv-oe"] == {
        "updatedAt": (NOW + timedelta(minutes=26)).isoformat(),
        "source": "alerts.energy",
        "ok": True,
    }

    api.reset()
    for area in api.areas.values():
        area["updated"] = "2026-11-06T11:00:00.000Z"
    next_run(operators, tmp_path, 80, **kwargs)
    assert len(api.calls) == 12
    times = [at for _, at in api.calls]
    assert all(b - a >= 0.2 - 1e-9 for a, b in zip(times, times[1:], strict=False))


def test_alerts_refetches_what_changed_is_two_hours_old_or_lacks_a_file_for_today(tmp_path):
    api = FakeAlerts()
    operators = all_alerts_operators()
    kwargs = {"alerts_get": api.get, "clock": api.clock.time, "sleep": api.clock.sleep}
    alerts_run(operators, tmp_path, api)

    def asked_at(minutes):
        api.reset()
        next_run(operators, tmp_path, minutes, **kwargs)
        return api.paths()

    api.areas["poltavska-oblast"]["mode"] = "scheduled"
    assert asked_at(30) == [OVERVIEW, shutdowns_path("poltavska-oblast")]

    (tmp_path / "out" / "kharkiv-oe.json").unlink()
    assert asked_at(60) == [OVERVIEW, shutdowns_path("kharkivska-oblast")]

    (tmp_path / "out" / "zaporizhzhia-oe.json").write_text(OLD_FILE.replace("2026-11-06", "2026-11-05"))
    assert asked_at(90) == [OVERVIEW, shutdowns_path("zaporizka-oblast")]

    index_path = tmp_path / "out" / "index.json"
    index = json.loads(index_path.read_text())
    index["dtek-krem"]["source"] = "Working"
    index_path.write_text(json.dumps(index))
    assert asked_at(116) == [OVERVIEW, shutdowns_path("kyivska-oblast")]

    fetched_at_start = ["chernivecka-oblast"] + [slug for slug in ALERTS_FALLBACK.values() if slug != "kyivska-oblast"]
    assert asked_at(145) == [OVERVIEW] + [shutdowns_path(slug) for slug in fetched_at_start]


def test_alerts_pass_stops_on_429_and_on_a_low_daily_quota(tmp_path):
    poltava = shutdowns_path("poltavska-oblast")
    order = list(ALERTS_ONLY.values()) + list(ALERTS_FALLBACK.values())
    until_poltava = [OVERVIEW] + [shutdowns_path(slug) for slug in order[:2]]
    assert until_poltava[-1] == poltava

    for name in ("429", "quota"):
        directory = tmp_path / name
        directory.mkdir()
        api = FakeAlerts()
        api.delay = 45
        if name == "429":
            api.status[poltava], api.headers[poltava] = 429, {"retry-after": "3600"}
        else:
            api.headers[poltava] = {"x-ratelimit-daily-remaining": "1999"}
        operators = all_alerts_operators()
        kwargs = {"alerts_get": api.get, "clock": api.clock.time, "sleep": api.clock.sleep}

        index, out = run(operators, directory, **kwargs)

        assert api.paths() == until_poltava, name
        assert index["kharkiv-oe"]["ok"] is True, name
        assert index["poltava-oe"]["ok"] is (name == "quota"), name
        assert index["zaporizhzhia-oe"] == {"updatedAt": None, "source": None, "ok": False}, name
        assert not (out / "zaporizhzhia-oe.json").exists(), name
        assert ("alertsRetryAfter" in index) == (name == "429"), name
        if name == "429":
            replied_at = NOW + timedelta(seconds=3 * 45 + 2 * 0.2)
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
    (previous / "index.json").write_text(
        json.dumps({"poltava-oe": {"updatedAt": OLD_STAMP, "source": "alerts.energy", "ok": True}})
    )

    index, out = alerts_run(all_alerts_operators(), tmp_path, api)

    assert index["poltava-oe"] == {"updatedAt": OLD_STAMP, "source": "alerts.energy", "ok": False}
    assert (out / "poltava-oe.json").read_text() == OLD_FILE
    assert len(api.calls) == 12
    others = [op_id for op_id in SLUGS if op_id != "poltava-oe"]
    assert all(index[op_id]["ok"] and (out / f"{op_id}.json").exists() for op_id in others)


def test_alerts_overview_it_cannot_read_is_logged(tmp_path, capsys):
    api = FakeAlerts()
    del api.areas["sumska-oblast"]["updated"]

    index, _ = alerts_run(all_alerts_operators(), tmp_path / "no-time", api)

    assert "sumy-oe: alerts.energy overview has no updated time for sumska-oblast" in capsys.readouterr().err
    assert shutdowns_path("sumska-oblast") not in api.paths()
    assert index["sumy-oe"]["ok"] is False and index["kharkiv-oe"]["ok"] is True

    api.reset()
    api.overview = {"areas": list(api.areas.values())}

    index, _ = alerts_run(all_alerts_operators(), tmp_path / "wrapped", api)

    assert "alerts.energy: overview is dict, not a list" in capsys.readouterr().err
    assert api.paths() == [OVERVIEW]
    assert not any(index[op_id]["ok"] for op_id in SLUGS)


def test_alerts_pass_keeps_what_it_fetched_before_the_deadline(tmp_path):
    api = FakeAlerts()
    api.delay = 50

    index, out = alerts_run(all_alerts_operators(), tmp_path, api, alerts_timeout=120)

    assert api.paths() == [OVERVIEW, shutdowns_path("kharkivska-oblast"), shutdowns_path("poltavska-oblast")]
    assert index["kharkiv-oe"]["ok"] is True and index["poltava-oe"]["ok"] is True
    assert today_slots(out / "kharkiv-oe.json") == [{"start": 0, "end": 240, "type": "Definite"}]
    assert sorted(index["alertsAreas"]) == ["kharkivska-oblast", "poltavska-oblast"]
    assert index["zaporizhzhia-oe"]["ok"] is False and not (out / "zaporizhzhia-oe.json").exists()


def test_alerts_without_a_key_is_skipped_and_the_run_still_succeeds(tmp_path, monkeypatch):
    def no_network(*args):
        raise AssertionError("alerts.energy must not be called without a key")

    monkeypatch.delenv("ALERTS_ENERGY_KEY", raising=False)
    monkeypatch.setattr("outage_feed.collect.live_get", no_network)
    previous = tmp_path / "previous"
    previous.mkdir()
    (previous / "kharkiv-oe.json").write_text(OLD_FILE)

    index, out = run([operator("kharkiv-oe"), operator("sumy-oe", Failing()), operator("free", Working())], tmp_path)

    assert (out / "kharkiv-oe.json").read_text() == OLD_FILE
    assert index["kharkiv-oe"]["ok"] is False and index["sumy-oe"]["ok"] is False
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

    index, _ = run([operator("kharkiv-oe")], tmp_path)

    assert keys == ["secret-key"] and index["kharkiv-oe"]["ok"] is True
