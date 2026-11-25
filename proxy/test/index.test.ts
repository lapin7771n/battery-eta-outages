import { env } from "cloudflare:workers";
import { createExecutionContext, createScheduledController, waitOnExecutionContext } from "cloudflare:test";
import { afterEach, describe, expect, it, vi } from "vitest";
import worker, { type CronEnv } from "../src/index";

const T0 = Date.parse("2026-11-06T12:00:00Z");
const BASE = "https://proxy.test";

afterEach(() => {
  vi.restoreAllMocks();
});

describe("routes", () => {
  it("answers 410 gone for every path and method without calling upstream", async () => {
    const upstream = vi.spyOn(globalThis, "fetch").mockRejectedValue(new Error("unexpected fetch"));
    const cases: [string, string][] = [
      ["GET", "/settlements"],
      ["GET", "/settlements?area=chernivecka-oblast&q=Чернівці"],
      ["GET", "/streets"],
      ["GET", "/streets?settlement=UA73060610010033137&q=Головна"],
      ["GET", "/houses"],
      ["GET", "/houses?street=6ae0636cfffe6111eb2b628a"],
      ["GET", "/queues"],
      ["GET", "/queues?street=6ae0636cfffe6111eb2b628a&house=3&area=chernivecka-oblast"],
      ["GET", "/other"],
      ["GET", "/other?x=1"],
      ["POST", "/settlements"],
      ["POST", "/other?x=1"],
    ];

    for (const [method, path] of cases) {
      const ctx = createExecutionContext();
      const reply = await worker.fetch(new Request(BASE + path, { method }), env, ctx);
      await waitOnExecutionContext(ctx);
      expect([method, path, reply.status, await reply.json()]).toEqual([method, path, 410, { error: "gone" }]);
    }
    expect(upstream).not.toHaveBeenCalled();
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
