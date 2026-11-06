import asyncio
import json
from datetime import date

from ua_outages.http import FetchError
from ua_outages.models import DaySchedule, Outage, at
from ua_outages.providers.base import AddressSource, Provider
from ua_outages.registry import Operator

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


def operator(op_id, *sources):
    return Operator(op_id, op_id, "region", "https://example.test", sources=sources, groups=("1.1",))


def run(operators, tmp_path, **kwargs):
    previous, out = tmp_path / "previous", tmp_path / "out"
    return asyncio.run(collect(operators, out, previous, now=NOW, **kwargs)), out


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
