import { env, exports } from "cloudflare:workers";
import { createExecutionContext, createScheduledController, runInDurableObject, waitOnExecutionContext } from "cloudflare:test";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import worker, { type CronEnv } from "../src/index";
import houses from "./fixtures/houses-holovna.json";
import queuesHolovna from "./fixtures/queues-holovna-3.json";
import queuesIrpin from "./fixtures/queues-irpin-multi.json";
import queuesRailway from "./fixtures/queues-zaporizhzhia-uz.json";
import settlements from "./fixtures/settlement-search-chernivtsi.json";
import streets from "./fixtures/street-search-holovna.json";

const T0 = Date.parse("2026-11-06T12:00:00Z");
const BASE = "https://proxy.test";
const HOLOVNA = "6ae0636cfffe6111eb2b628a";
const KATOTTG = "UA73060610010033137";
const OTHER_STREET = "6a44c7f278b52d019388ada3";
const KEY = "test-key-0000";

const DAY_MS = 86_400_000;

let nextIp = 1;
let upstream: ReturnType<typeof mockUpstream>;

type Handler = (url: URL, headers: Headers) => Response | Promise<Response>;

function mockUpstream(handler: Handler) {
  return vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
    const href = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    return handler(new URL(href), new Headers(init?.headers));
  });
}

function ok(body: unknown, headers: Record<string, string> = {}): Response {
  return Response.json(body, { headers });
}

async function get(path: string, ip = `10.0.0.${nextIp++}`): Promise<Response> {
  const ctx = createExecutionContext();
  const reply = await worker.fetch(
    new Request(BASE + path, { headers: { "CF-Connecting-IP": ip } }),
    env,
    ctx,
  );
  await waitOnExecutionContext(ctx);
  return reply;
}

let nextQuery = 1;

function search(q = `Чернівці ${nextQuery++}`): string {
  return `/settlements?area=chernivecka-oblast&q=${encodeURIComponent(q)}`;
}

async function resetGate(): Promise<void> {
  await runInDurableObject(exports.Gate.getByName("global"), (_gate, state) => {
    state.storage.sql.exec("DELETE FROM hits");
    state.storage.sql.exec("DELETE FROM cache");
    state.storage.sql.exec("UPDATE state SET paused_until = 0");
  });
}

async function sha256(data: string | Uint8Array): Promise<string> {
  const bytes = typeof data === "string" ? new TextEncoder().encode(data) : data;
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
  return Array.from(digest, (b) => b.toString(16).padStart(2, "0")).join("");
}

function secrets(ip: string): RegExp {
  const parts = ["Таємна", encodeURIComponent("Таємна"), ip, KEY];
  return new RegExp(parts.map((part) => part.replace(/[.%]/g, "\\$&")).join("|"));
}

function inGate<T>(read: (sql: SqlStorage) => T): Promise<T> {
  return runInDurableObject(exports.Gate.getByName("global"), (_gate, state) => read(state.storage.sql));
}

beforeEach(async () => {
  vi.setSystemTime(T0);
  await resetGate();
});

afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
});

describe("routes", () => {
  it("/settlements keeps the area, caps at 40 and calls upstream with the key", async () => {
    upstream = mockUpstream(() => ok(settlements));
    const timeout = vi.spyOn(AbortSignal, "timeout");

    const chernivtsi = await get("/settlements?area=chernivecka-oblast&q=Чернівці");
    expect(await chernivtsi.json()).toEqual([{ id: KATOTTG, name: "м. Чернівці" }]);

    const call = upstream.mock.calls[0]!;
    const url = new URL(call[0] as string);
    expect(url.origin + url.pathname).toBe("https://alerts.energy/api/v1/atus/settlement-search");
    expect(url.searchParams.get("q")).toBe("Чернівці");
    expect(new Headers(call[1]?.headers).get("Authorization")).toBe(`Bearer ${KEY}`);
    expect(timeout).toHaveBeenCalledWith(10_000);
    expect(call[1]?.signal).toBe(timeout.mock.results[0]!.value);

    const row = settlements[0]!;
    upstream.mockImplementation(async () => ok(Array.from({ length: 45 }, () => row)));
    const many = (await (await get(search())).json()) as unknown[];
    expect(many).toHaveLength(40);
  });

  it("/streets maps value and label and sends katottg", async () => {
    upstream = mockUpstream(() => ok(streets));

    const reply = await get(`/streets?settlement=${KATOTTG}&q=Головна`);

    expect(await reply.json()).toEqual([{ id: HOLOVNA, name: "вулиця Головна" }]);
    const url = new URL(upstream.mock.calls[0]![0] as string);
    expect(url.pathname).toBe("/api/v1/address-directory/street-search");
    expect(url.searchParams.get("katottg")).toBe(KATOTTG);
    expect(url.searchParams.get("q")).toBe("Головна");
  });

  it("/houses parses the JSON string in value", async () => {
    upstream = mockUpstream(() => ok(houses));

    const reply = await get(`/houses?street=${HOLOVNA}`);
    const list = (await reply.json()) as { house: string; building: string; name: string }[];

    expect(new URL(upstream.mock.calls[0]![0] as string).pathname).toBe(
      `/api/v1/address-directory/streets/${HOLOVNA}/houses`,
    );
    expect(list).toHaveLength(houses.length);
    expect(list[0]).toEqual({ house: "3", building: "", name: "3" });
    expect(list).toContainEqual({ house: "220В", building: "1", name: "220В, корпус 1" });
  });

  it("drops rows the app cannot use and caps names and lists", async () => {
    const streetId = (i: number) => i.toString(16).padStart(24, "0");
    const row = settlements[0]!;
    upstream = mockUpstream((url) => {
      if (url.pathname.endsWith("/street-search")) {
        return ok([
          { value: "not-an-id", label: "вулиця Погана" },
          { value: streetId(1) },
          { value: streetId(2), label: "я".repeat(250) },
          ...Array.from({ length: 600 }, (_, i) => ({ value: streetId(i + 3), label: `вулиця ${i}` })),
        ]);
      }
      if (url.pathname.endsWith("/houses")) {
        return ok([
          { value: "not json", label: "1" },
          { value: JSON.stringify(["1".repeat(21), ""]), label: "довгий" },
          { value: JSON.stringify(["5", "2"]), label: "5, корпус 2" },
          ...Array.from({ length: 600 }, (_, i) => ({ value: JSON.stringify([String(i + 6), ""]), label: `${i + 6}` })),
        ]);
      }
      return ok([{ ...row, settlement: { ...row.settlement, katottg: "UA123" } }]);
    });

    const streetList = (await (await get(`/streets?settlement=${KATOTTG}&q=Головна`)).json()) as unknown[];
    expect(streetList).toHaveLength(500);
    expect(streetList[0]).toEqual({ id: streetId(2), name: "я".repeat(200) });
    const houseList = (await (await get(`/houses?street=${HOLOVNA}`)).json()) as unknown[];
    expect(houseList).toHaveLength(500);
    expect(houseList[0]).toEqual({ house: "5", building: "2", name: "5, корпус 2" });
    expect(await (await get(search())).json()).toEqual([]);
  });

  it("/queues keeps only the area's oblenergo rows, without repeats, in ascending order", async () => {
    const queues = async (reply: unknown, area: string, house: string, building = "") => {
      upstream = mockUpstream(() => ok(reply));
      const path = `/queues?street=${OTHER_STREET}&house=${encodeURIComponent(house)}&building=${building}&area=${area}`;
      const answer = await get(path);
      upstream.mockRestore();
      return answer.json();
    };

    expect(await queues(queuesIrpin, "kyivska-oblast", "1")).toEqual({ queues: ["2.2", "6.1", "6.2"] });
    expect(await queues(queuesHolovna, "chernivecka-oblast", "3")).toEqual({ queues: ["8"] });
    expect(await queues(queuesRailway, "zaporizka-oblast", "1Г")).toEqual({ queues: [] });
    const mixed = {
      assignments: [
        { supplier_slug: "odeska_oblast_oblenergo", queue: "10.1" },
        { supplier_slug: "odeska_oblast_oblenergo", queue: "2.1" },
        { supplier_slug: "odeska_oblast_oblenergo", queue: "2.1" },
        { supplier_slug: "odeska_oblast_oblenergo", queue: "1.2" },
        { supplier_slug: "odeska_oblast_uz", queue: "5.2" },
      ],
    };
    expect(await queues(mixed, "odeska-oblast", "1", "А")).toEqual({ queues: ["1.2", "2.1", "10.1"] });

    upstream = mockUpstream(() => ok(queuesHolovna));
    await get(`/queues?street=${HOLOVNA}&house=7%2F9&building=%D0%90&area=chernivecka-oblast`);
    const url = new URL(upstream.mock.calls[0]![0] as string);
    expect(url.pathname).toBe(`/api/v1/address-directory/streets/${HOLOVNA}/queues`);
    expect(url.searchParams.get("house")).toBe("7/9");
    expect(url.searchParams.get("building")).toBe("А");
  });
});

describe("validation", () => {
  it("answers 400 on bad input and 404 on other paths or methods, without calling upstream", async () => {
    upstream = mockUpstream(() => ok([]));
    const long = "я".repeat(61);
    const bad = [
      "/settlements?area=chernivecka-oblast",
      "/settlements?area=chernivecka-oblast&q=Ч",
      `/settlements?area=chernivecka-oblast&q=${long}`,
      "/settlements?area=lvivska-oblast&q=Львів",
      "/settlements?q=Львів",
      `/streets?settlement=UA1234&q=Головна`,
      `/streets?settlement=${KATOTTG}&q=Г`,
      "/houses?street=XYZ",
      `/houses?street=${HOLOVNA.toUpperCase()}`,
      `/queues?street=${HOLOVNA}&house=&area=chernivecka-oblast`,
      `/queues?street=${HOLOVNA}&house=${"1".repeat(21)}&area=chernivecka-oblast`,
      `/queues?street=${HOLOVNA}&house=1&building=${"1".repeat(21)}&area=chernivecka-oblast`,
      `/queues?street=${HOLOVNA}&house=1&area=lvivska-oblast`,
    ];
    for (const path of bad) expect([path, (await get(path)).status]).toEqual([path, 400]);

    expect((await get("/other")).status).toBe(404);
    const post = await worker.fetch(new Request(BASE + search(), { method: "POST" }), env, createExecutionContext());
    expect(post.status).toBe(404);
    expect(upstream).not.toHaveBeenCalled();
  });
});

describe("limits", () => {
  it("answers 429 once one IP has sent 30 requests within the last hour", async () => {
    upstream = mockUpstream(() => ok(settlements));
    const ip = "203.0.113.7";
    const path = search();
    const send = async (count: number) => {
      for (let i = 0; i < count; i++) expect((await get(path, ip)).status).toBe(200);
    };

    await send(15);
    vi.setSystemTime(T0 + 30 * 60_000);
    await send(15);
    vi.setSystemTime(T0 + 59 * 60_000);
    expect((await get(path, ip)).status).toBe(429);
    expect((await get(path, "203.0.113.8")).status).toBe(200);

    vi.setSystemTime(T0 + 61 * 60_000);
    await send(15);
    expect((await get(path, ip)).status).toBe(429);
  });

  it("pauses upstream until the reset time when daily-remaining drops below 3000", async () => {
    let remaining = "2999";
    let reset = T0 / 1000 + 100;
    upstream = mockUpstream(() =>
      ok(settlements, {
        "x-ratelimit-daily-remaining": remaining,
        "x-ratelimit-daily-reset": String(reset),
      }),
    );

    expect((await get(search())).status).toBe(200);
    expect(upstream).toHaveBeenCalledTimes(1);

    const blocked = await get(search());
    expect(blocked.status).toBe(503);
    expect(blocked.headers.get("Retry-After")).toBe("100");
    expect(upstream).toHaveBeenCalledTimes(1);

    vi.setSystemTime(T0 + 101_000);
    remaining = "3000";
    reset = T0 / 1000 + 50_000;
    expect((await get(search())).status).toBe(200);
    expect((await get(search())).status).toBe(200);
    expect(upstream).toHaveBeenCalledTimes(3);
  });

  it("pauses for at most 24 hours, and for 24 hours when the reset header is unusable", async () => {
    const floor = (reset?: string) => () =>
      ok(settlements, {
        "x-ratelimit-daily-remaining": "10",
        ...(reset === undefined ? {} : { "x-ratelimit-daily-reset": reset }),
      });
    const replies: [string, () => Response][] = [
      ["missing", floor()],
      ["past", floor(String(T0 / 1000 - 60))],
      ["seconds until reset", floor("43200")],
      ["not a number", floor("soon")],
      ["milliseconds", floor(String(T0 + 100_000))],
      ["long Retry-After", () => new Response("", { status: 429, headers: { "Retry-After": "999999" } })],
    ];

    for (const [name, reply] of replies) {
      await resetGate();
      upstream = mockUpstream(reply);
      await get(search());
      const blocked = await get(search());
      expect([name, blocked.status, blocked.headers.get("Retry-After"), upstream.mock.calls.length]).toEqual([
        name,
        503,
        String(DAY_MS / 1000),
        1,
      ]);
      upstream.mockRestore();
    }
  });

  it("turns an upstream 429 into 503 and stays away until Retry-After passes", async () => {
    let limited = true;
    upstream = mockUpstream(() =>
      limited ? new Response("slow down", { status: 429, headers: { "Retry-After": "60" } }) : ok(settlements),
    );

    const first = await get(search());
    expect(first.status).toBe(503);
    expect(first.headers.get("Retry-After")).toBe("60");
    expect(upstream).toHaveBeenCalledTimes(1);

    vi.setSystemTime(T0 + 30_000);
    expect((await get(search())).status).toBe(503);
    expect(upstream).toHaveBeenCalledTimes(1);

    vi.setSystemTime(T0 + 61_000);
    limited = false;
    expect((await get(search())).status).toBe(200);
    expect(upstream).toHaveBeenCalledTimes(2);
  });

  it("waits one hour after an upstream 429 without Retry-After", async () => {
    upstream = mockUpstream(() => new Response("", { status: 429 }));
    await get(search());

    vi.setSystemTime(T0 + 3_599_000);
    expect((await get(search())).status).toBe(503);
    expect(upstream).toHaveBeenCalledTimes(1);

    vi.setSystemTime(T0 + 3_601_000);
    await get(search());
    expect(upstream).toHaveBeenCalledTimes(2);
  });
});

describe("gate", () => {
  it("never shortens a pause that is already set", async () => {
    const gate = exports.Gate.getByName("global");

    await gate.pause(T0 + 10_000);
    await gate.pause(T0 + 5_000);

    expect((await gate.admit("198.51.100.1", "none", T0)).pausedUntil).toBe(T0 + 10_000);
  });

  it("stores a salted SHA-256 of the IP and never the IP", async () => {
    const ip = "203.0.113.9";
    await exports.Gate.getByName("global").admit(ip, "none", T0);

    const { salt, stored } = await inGate((sql) => ({
      salt: new Uint8Array(sql.exec<{ value: ArrayBuffer }>("SELECT value FROM salt").one().value),
      stored: sql.exec<{ ip: string }>("SELECT ip FROM hits").toArray().map((row) => row.ip),
    }));
    const ipBytes = new TextEncoder().encode(ip);
    const salted = new Uint8Array([...salt, ...ipBytes]);

    expect(salt).toHaveLength(32);
    expect(stored).toEqual([await sha256(salted)]);
  });

  it("does not cache a body over 1 MB", async () => {
    const gate = exports.Gate.getByName("global");

    await gate.store("big", "x".repeat(1_048_577), T0);
    await gate.store("limit", "x".repeat(1_048_576), T0);

    expect((await gate.admit("198.51.100.3", "big", T0)).cached).toBeNull();
    expect((await gate.admit("198.51.100.3", "limit", T0)).cached).toHaveLength(1_048_576);
  });
});

describe("failures", () => {
  it("answers 502 on a 5xx, a timeout, a failed body read, a body over 1 MB or not JSON, and does not pause", async () => {
    const log = vi.spyOn(console, "log");
    const replies: (() => Response)[] = [
      () => new Response("[]", { status: 500 }),
      () => {
        throw new DOMException("timed out", "TimeoutError");
      },
      () =>
        new Response(
          new ReadableStream({
            pull(controller) {
              controller.error(new Error("connection reset"));
            },
          }),
        ),
      () => new Response("[" + '"x",'.repeat(300_000) + '"x"]'),
      () => new Response("<html>", { status: 200 }),
    ];
    const count = replies.length;
    upstream = mockUpstream(() => replies.shift()!());

    for (let i = 0; i < count; i++) {
      const reply = await get(search(`Таємна ${i}`), "198.51.100.77");
      expect([i, reply.status, await reply.json()]).toEqual([i, 502, { error: "bad gateway" }]);
    }
    expect(upstream).toHaveBeenCalledTimes(count);
    expect(log.mock.calls.flat().join("\n")).not.toMatch(secrets("198.51.100.77"));

    upstream.mockImplementation(async () => ok(settlements));
    expect((await get(search())).status).toBe(200);
  });

  it("answers 502 without details when the Gate storage fails", async () => {
    upstream = mockUpstream(() => ok(settlements));
    const log = vi.spyOn(console, "log");
    await inGate((sql) => sql.exec("DROP TABLE hits"));
    try {
      const reply = await get(search("Таємна вулиця"), "198.51.100.66");
      expect([reply.status, await reply.json()]).toEqual([502, { error: "bad gateway" }]);
    } finally {
      await inGate((sql) => {
        sql.exec("CREATE TABLE hits (ip TEXT NOT NULL, at INTEGER NOT NULL)");
        sql.exec("CREATE INDEX hits_ip_at ON hits (ip, at)");
        sql.exec("CREATE INDEX hits_at ON hits (at)");
      });
    }
    expect(upstream).not.toHaveBeenCalled();
    expect(log.mock.calls.flat().join("\n")).not.toMatch(secrets("198.51.100.66"));
  });
});

describe("cache", () => {
  it("keeps upstream bodies in the Gate for 24 hours under a hashed key, for every user and area", async () => {
    vi.spyOn(caches.default, "match").mockResolvedValue(undefined);
    vi.spyOn(caches.default, "put").mockResolvedValue(undefined);
    upstream = mockUpstream(() => ok(settlements));
    const path = `/settlements?area=chernivecka-oblast&q=${encodeURIComponent("Чернівці кеш")}`;
    const cached = () => inGate((sql) => sql.exec<{ key: string; expires: number }>("SELECT key, expires FROM cache").toArray());

    const first = await get(path);
    const second = await get(path);
    const other = await get(path.replace("chernivecka-oblast", "kyivska-oblast"));

    expect(upstream).toHaveBeenCalledTimes(1);
    expect(await first.json()).toEqual([{ id: KATOTTG, name: "м. Чернівці" }]);
    expect(await second.json()).toEqual([{ id: KATOTTG, name: "м. Чернівці" }]);
    expect(await other.json()).toEqual([]);
    const key = await sha256(upstream.mock.calls[0]![0] as string);
    expect(await cached()).toEqual([{ key, expires: T0 + DAY_MS }]);

    vi.setSystemTime(T0 + DAY_MS);
    await get(search());
    expect((await cached()).map((row) => row.key)).not.toContain(key);
    await get(path);
    expect(upstream).toHaveBeenCalledTimes(3);
  });
});

describe("feed cron", () => {
  async function tick(token?: string) {
    const ctx = createExecutionContext();
    const withToken: CronEnv = { ...env, GITHUB_DISPATCH_TOKEN: token };
    await worker.scheduled(createScheduledController({ scheduledTime: T0, cron: "*/10 * * * *" }), withToken, ctx);
    await waitOnExecutionContext(ctx);
  }

  it("starts outage-feed.yml on main with the token, and sends nothing without one", async () => {
    const calls: { url: string; method: string; auth: string | null; body: unknown }[] = [];
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
      calls.push({ url, method: init?.method ?? "GET", auth: new Headers(init?.headers).get("Authorization"), body: JSON.parse(String(init?.body)) });
      return new Response(null, { status: 204 });
    });

    await tick();
    expect(calls).toEqual([]);

    await tick("gh-test-token");
    expect(calls).toEqual([
      {
        url: "https://api.github.com/repos/lapin7771n/battery-eta-outages/actions/workflows/outage-feed.yml/dispatches",
        method: "POST",
        auth: "Bearer gh-test-token",
        body: { ref: "main" },
      },
    ]);
  });

  it("logs a failed dispatch without the token and does not throw", async () => {
    const lines: string[] = [];
    vi.spyOn(console, "log").mockImplementation((line: string) => void lines.push(line));
    vi.spyOn(globalThis, "fetch").mockRejectedValue(new Error("network down gh-test-token"));
    await tick("gh-test-token");
    vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response("{}", { status: 401 }));
    await tick("gh-test-token");
    expect(lines).toEqual([JSON.stringify({ event: "dispatch_failed" }), JSON.stringify({ event: "dispatch", status: 401 })]);
  });
});
