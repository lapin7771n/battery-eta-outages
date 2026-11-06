import type { Gate } from "./gate";

export { Gate } from "./gate";

const UPSTREAM = "https://alerts.energy/api/v1";
const UPSTREAM_TIMEOUT_MS = 10_000;
const CACHE_SECONDS = 86_400;
const DAILY_FLOOR = 3000;
const DEFAULT_PAUSE_SECONDS = 3600;
const MAX_PAUSE_SECONDS = 86_400;
const MAX_SETTLEMENTS = 40;

const AREAS = new Set([
  "kyivska-oblast",
  "volynska-oblast",
  "ivano-frankivska-oblast",
  "kirovogradska-oblast",
  "odeska-oblast",
  "rivnenska-oblast",
  "sumska-oblast",
  "khmelnytska-oblast",
  "cherkaska-oblast",
  "kharkivska-oblast",
  "zaporizka-oblast",
  "chernivecka-oblast",
]);

interface SettlementRow {
  settlement?: { katottg?: string; full_name?: string };
  ancestors?: { slug?: string }[];
}
interface ValueLabel {
  value?: string;
  label?: string;
}
interface QueuesReply {
  assignments?: { supplier_slug?: string; queue?: string }[];
}

interface Route {
  upstream: string;
  shape: (data: unknown) => unknown;
}

function json(body: unknown, status = 200, headers: Record<string, string> = {}): Response {
  return Response.json(body, { status, headers });
}

function between(value: string | null, min: number, max: number): value is string {
  if (value === null) return false;
  const length = [...value].length;
  return length >= min && length <= max;
}

function upstreamUrl(path: string, params: Record<string, string> = {}): string {
  const url = new URL(UPSTREAM + path);
  for (const [name, value] of Object.entries(params)) url.searchParams.set(name, value);
  return url.toString();
}

function rows<T>(data: unknown): T[] {
  if (!Array.isArray(data)) throw new Error("expected an array");
  return data;
}

function route(url: URL): Route | Response {
  const p = url.searchParams;
  const area = p.get("area");
  const q = p.get("q");
  switch (url.pathname) {
    case "/settlements": {
      if (area === null || !AREAS.has(area) || !between(q, 2, 60)) return bad();
      return {
        upstream: upstreamUrl("/atus/settlement-search", { q }),
        shape: (data) =>
          rows<SettlementRow>(data)
            .filter((row) => row.ancestors?.[0]?.slug === area)
            .slice(0, MAX_SETTLEMENTS)
            .map((row) => ({ id: row.settlement?.katottg, name: row.settlement?.full_name })),
      };
    }
    case "/streets": {
      const settlement = p.get("settlement");
      if (settlement === null || !/^UA\d{17}$/.test(settlement) || !between(q, 2, 60)) return bad();
      return {
        upstream: upstreamUrl("/address-directory/street-search", { katottg: settlement, q }),
        shape: (data) => rows<ValueLabel>(data).map((row) => ({ id: row.value, name: row.label })),
      };
    }
    case "/houses": {
      const street = p.get("street");
      if (street === null || !/^[0-9a-f]{24}$/.test(street)) return bad();
      return {
        upstream: upstreamUrl(`/address-directory/streets/${street}/houses`),
        shape: (data) =>
          rows<ValueLabel>(data).flatMap((row) => {
            const parts: unknown = JSON.parse(row.value ?? "[]");
            if (!Array.isArray(parts) || typeof parts[0] !== "string") return [];
            const building = typeof parts[1] === "string" ? parts[1] : "";
            return [{ house: parts[0], building, name: row.label }];
          }),
      };
    }
    case "/queues": {
      const street = p.get("street");
      const house = p.get("house");
      const building = p.get("building") ?? "";
      if (
        area === null ||
        !AREAS.has(area) ||
        street === null ||
        !/^[0-9a-f]{24}$/.test(street) ||
        !between(house, 1, 20) ||
        !between(building, 0, 20)
      ) {
        return bad();
      }
      const supplier = area.replaceAll("-", "_") + "_oblenergo";
      return {
        upstream: upstreamUrl(`/address-directory/streets/${street}/queues`, { house, building }),
        shape: (data) => {
          const reply: QueuesReply = typeof data === "object" && data !== null ? data : {};
          const found = (reply.assignments ?? []).flatMap((a) =>
            a.supplier_slug === supplier && typeof a.queue === "string" ? [a.queue] : [],
          );
          const queues = [...new Set(found)].sort((a, b) => a.localeCompare(b, "en", { numeric: true }));
          return { queues };
        },
      };
    }
    default:
      return json({ error: "not found" }, 404);
  }
}

function bad(): Response {
  return json({ error: "bad request" }, 400);
}

function paused(until: number, now: number): Response {
  return json({ error: "upstream paused" }, 503, {
    "Retry-After": String(Math.max(1, Math.ceil((until - now) / 1000))),
  });
}

function headerNumber(headers: Headers, name: string): number | null {
  const raw = headers.get(name);
  if (raw === null || raw.trim() === "") return null;
  const value = Number(raw);
  return Number.isFinite(value) ? value : null;
}

async function fromUpstream(
  target: string,
  env: Env,
  gate: DurableObjectStub<Gate>,
  now: number,
): Promise<{ text: string } | Response> {
  let reply: Response;
  try {
    reply = await fetch(target, {
      headers: { Authorization: `Bearer ${env.ALERTS_ENERGY_KEY}`, Accept: "application/json" },
      signal: AbortSignal.timeout(UPSTREAM_TIMEOUT_MS),
    });
  } catch {
    console.log(JSON.stringify({ event: "upstream_unreachable" }));
    return json({ error: "bad gateway" }, 502);
  }

  if (reply.status === 429) {
    const asked = headerNumber(reply.headers, "Retry-After");
    const seconds = Math.min(asked !== null && asked > 0 ? asked : DEFAULT_PAUSE_SECONDS, MAX_PAUSE_SECONDS);
    const until = now + seconds * 1000;
    await gate.pause(until);
    console.log(JSON.stringify({ event: "upstream_429", seconds }));
    return paused(until, now);
  }
  if (!reply.ok) {
    console.log(JSON.stringify({ event: "upstream_error", status: reply.status }));
    return json({ error: "bad gateway" }, 502);
  }

  const remaining = headerNumber(reply.headers, "x-ratelimit-daily-remaining");
  if (remaining !== null && remaining < DAILY_FLOOR) {
    const reset = headerNumber(reply.headers, "x-ratelimit-daily-reset");
    await gate.pause(reset !== null ? reset * 1000 : now + MAX_PAUSE_SECONDS * 1000);
    console.log(JSON.stringify({ event: "daily_floor", remaining }));
  }
  return { text: await reply.text() };
}

export default {
  async fetch(request, env, ctx): Promise<Response> {
    const url = new URL(request.url);
    if (request.method !== "GET") return json({ error: "not found" }, 404);
    const found = route(url);
    if (found instanceof Response) return found;

    const now = Date.now();
    const gate = ctx.exports.Gate.getByName("global");
    const admission = await gate.admit(request.headers.get("CF-Connecting-IP") ?? "unknown", now);
    if (admission.limited) return json({ error: "too many requests" }, 429);

    const cacheKey = new Request(found.upstream);
    const cached = await caches.default.match(cacheKey);
    let text: string;
    if (cached) {
      text = await cached.text();
    } else {
      if (admission.pausedUntil > now) return paused(admission.pausedUntil, now);
      const result = await fromUpstream(found.upstream, env, gate, now);
      if (result instanceof Response) return result;
      text = result.text;
    }

    let body: unknown;
    try {
      body = found.shape(JSON.parse(text));
    } catch {
      console.log(JSON.stringify({ event: "upstream_bad_shape" }));
      return json({ error: "bad gateway" }, 502);
    }
    if (!cached) {
      await caches.default.put(
        cacheKey,
        new Response(text, {
          headers: { "Content-Type": "application/json", "Cache-Control": `public, max-age=${CACHE_SECONDS}` },
        }),
      );
    }
    return json(body);
  },
} satisfies ExportedHandler<Env>;
