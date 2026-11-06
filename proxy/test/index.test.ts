import { env, exports } from "cloudflare:workers";
import { createExecutionContext, runInDurableObject, waitOnExecutionContext } from "cloudflare:test";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import worker from "../src/index";
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
    state.storage.sql.exec("UPDATE state SET paused_until = 0");
  });
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

    const chernivtsi = await get("/settlements?area=chernivecka-oblast&q=Чернівці");
    expect(await chernivtsi.json()).toEqual([{ id: KATOTTG, name: "м. Чернівці" }]);

    const call = upstream.mock.calls[0]!;
    const url = new URL(call[0] as string);
    expect(url.origin + url.pathname).toBe("https://alerts.energy/api/v1/atus/settlement-search");
    expect(url.searchParams.get("q")).toBe("Чернівці");
    expect(new Headers(call[1]?.headers).get("Authorization")).toBe(`Bearer ${KEY}`);
    expect(call[1]?.signal).toBeInstanceOf(AbortSignal);

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
  it("answers 429 to the 31st request from one IP within an hour", async () => {
    upstream = mockUpstream(() => ok(settlements));
    const ip = "203.0.113.7";
    const path = search();

    for (let i = 1; i <= 30; i++) expect((await get(path, ip)).status).toBe(200);
    expect((await get(path, ip)).status).toBe(429);
    expect((await get(path, "203.0.113.8")).status).toBe(200);

    vi.setSystemTime(T0 + 3_601_000);
    expect((await get(path, ip)).status).toBe(200);
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

  it("pauses for 24 hours when the reset header is missing", async () => {
    upstream = mockUpstream(() => ok(settlements, { "x-ratelimit-daily-remaining": "10" }));
    await get(search());

    vi.setSystemTime(T0 + 23 * 3_600_000);
    expect((await get(search())).status).toBe(503);
    vi.setSystemTime(T0 + 25 * 3_600_000);
    expect((await get(search())).status).toBe(200);
    expect(upstream).toHaveBeenCalledTimes(2);
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

    expect((await gate.admit("198.51.100.1", T0)).pausedUntil).toBe(T0 + 10_000);
  });
});

describe("upstream failures", () => {
  it("answers 502 on a 5xx, a timeout or a body that is not JSON, and does not pause", async () => {
    const replies: (() => Response)[] = [
      () => new Response("[]", { status: 500 }),
      () => {
        throw new DOMException("timed out", "TimeoutError");
      },
      () => new Response("<html>", { status: 200 }),
    ];
    upstream = mockUpstream(() => replies.shift()!());

    for (let i = 0; i < 3; i++) {
      const reply = await get(search());
      expect(reply.status).toBe(502);
      expect(await reply.text()).not.toContain(KEY);
    }
    expect(upstream).toHaveBeenCalledTimes(3);

    upstream.mockImplementation(async () => ok(settlements));
    expect((await get(search())).status).toBe(200);
  });
});

describe("cache", () => {
  it("serves the second identical request from the cache, whatever the area", async () => {
    upstream = mockUpstream(() => ok(settlements));
    const q = encodeURIComponent("Чернівці кеш");

    const first = await get(`/settlements?area=chernivecka-oblast&q=${q}`);
    const second = await get(`/settlements?area=chernivecka-oblast&q=${q}`);
    const other = await get(`/settlements?area=kyivska-oblast&q=${q}`);

    expect(upstream).toHaveBeenCalledTimes(1);
    expect(await first.json()).toEqual([{ id: KATOTTG, name: "м. Чернівці" }]);
    expect(await second.json()).toEqual([{ id: KATOTTG, name: "м. Чернівці" }]);
    expect(await other.json()).toEqual([]);
  });
});
