import { sha256Hex, type Gate } from "./gate";

export { Gate } from "./gate";

const UPSTREAM = "https://alerts.energy/api/v1";
const UPSTREAM_TIMEOUT_MS = 10_000;
const DAILY_FLOOR = 3000;
const DEFAULT_PAUSE_SECONDS = 3600;
const MAX_PAUSE_MS = 86_400_000;
const MAX_SETTLEMENTS = 40;
const MAX_ROWS = 500;
const MAX_NAME = 200;
const MAX_BODY_BYTES = 1_048_576;
const SETTLEMENT_ID = /^UA\d{17}$/;
const STREET_ID = /^[0-9a-f]{24}$/;

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

function between(value: unknown, min: number, max: number): value is string {
  if (typeof value !== "string") return false;
  const length = [...value].length;
  return length >= min && length <= max;
}

function upstreamUrl(path: string, params: Record<string, string> = {}): string {
  const url = new URL(UPSTREAM + path);
  for (const [name, value] of Object.entries(params)) url.searchParams.set(name, value);
  return url.toString();
}

function rows<T extends object>(data: unknown): T[] {
  if (!Array.isArray(data)) throw new Error("expected an array");
  return data.filter((row): row is T => typeof row === "object" && row !== null);
}

function name(value: unknown): string | null {
  return typeof value === "string" ? [...value].slice(0, MAX_NAME).join("") : null;
}

function houseParts(value: unknown): unknown[] | null {
  if (typeof value !== "string") return null;
  try {
    const parts: unknown = JSON.parse(value);
    return Array.isArray(parts) ? parts : null;
  } catch {
    return null;
  }
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
            .flatMap((row) => {
              const id = row.settlement?.katottg;
              const label = name(row.settlement?.full_name);
              if (row.ancestors?.[0]?.slug !== area || typeof id !== "string" || !SETTLEMENT_ID.test(id) || label === null) return [];
              return [{ id, name: label }];
            })
            .slice(0, MAX_SETTLEMENTS),
      };
    }
    case "/streets": {
      const settlement = p.get("settlement");
      if (settlement === null || !SETTLEMENT_ID.test(settlement) || !between(q, 2, 60)) return bad();
      return {
        upstream: upstreamUrl("/address-directory/street-search", { katottg: settlement, q }),
        shape: (data) =>
          rows<ValueLabel>(data)
            .flatMap((row) => {
              const label = name(row.label);
              if (typeof row.value !== "string" || !STREET_ID.test(row.value) || label === null) return [];
              return [{ id: row.value, name: label }];
            })
            .slice(0, MAX_ROWS),
      };
    }
    case "/houses": {
      const street = p.get("street");
      if (street === null || !STREET_ID.test(street)) return bad();
      return {
        upstream: upstreamUrl(`/address-directory/streets/${street}/houses`),
        shape: (data) =>
          rows<ValueLabel>(data)
            .flatMap((row) => {
              const parts = houseParts(row.value);
              const label = name(row.label);
              if (parts === null || label === null) return [];
              const [house] = parts;
              const building = typeof parts[1] === "string" ? parts[1] : "";
              if (!between(house, 1, 20) || !between(building, 0, 20)) return [];
              return [{ house, building, name: label }];
            })
            .slice(0, MAX_ROWS),
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
        !STREET_ID.test(street) ||
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

async function pauseUpstream(gate: DurableObjectStub<Gate>, until: number, now: number): Promise<number> {
  const capped = Math.min(until, now + MAX_PAUSE_MS);
  await gate.pause(capped);
  return capped;
}

async function readCapped(reply: Response): Promise<string | null> {
  if (reply.body === null) return "";
  const reader = reply.body.getReader();
  const decoder = new TextDecoder();
  let size = 0;
  let text = "";
  for (let part = await reader.read(); !part.done; part = await reader.read()) {
    size += part.value.byteLength;
    if (size > MAX_BODY_BYTES) {
      await reader.cancel();
      return null;
    }
    text += decoder.decode(part.value, { stream: true });
  }
  return text + decoder.decode();
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
    const wait = (asked !== null && asked > 0 ? asked : DEFAULT_PAUSE_SECONDS) * 1000;
    const until = await pauseUpstream(gate, now + wait, now);
    console.log(JSON.stringify({ event: "upstream_429", seconds: (until - now) / 1000 }));
    return paused(until, now);
  }
  if (!reply.ok) {
    console.log(JSON.stringify({ event: "upstream_error", status: reply.status }));
    return json({ error: "bad gateway" }, 502);
  }

  const remaining = headerNumber(reply.headers, "x-ratelimit-daily-remaining");
  if (remaining !== null && remaining < DAILY_FLOOR) {
    const reset = headerNumber(reply.headers, "x-ratelimit-daily-reset");
    await pauseUpstream(gate, reset !== null && reset * 1000 > now ? reset * 1000 : now + MAX_PAUSE_MS, now);
    console.log(JSON.stringify({ event: "daily_floor", remaining }));
  }

  let text: string | null;
  try {
    text = await readCapped(reply);
  } catch {
    console.log(JSON.stringify({ event: "upstream_unreachable" }));
    return json({ error: "bad gateway" }, 502);
  }
  if (text === null) {
    console.log(JSON.stringify({ event: "upstream_too_large" }));
    return json({ error: "bad gateway" }, 502);
  }
  return { text };
}

async function handle(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
  if (request.method !== "GET") return json({ error: "not found" }, 404);
  const found = route(new URL(request.url));
  if (found instanceof Response) return found;

  const now = Date.now();
  const key = await sha256Hex(found.upstream);
  const gate = ctx.exports.Gate.getByName("global");
  const admission = await gate.admit(request.headers.get("CF-Connecting-IP") ?? "unknown", key, now);
  if (admission.limited) return json({ error: "too many requests" }, 429);

  let text = admission.cached;
  if (text === null) {
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
  if (admission.cached === null) await gate.store(key, text, now);
  return json(body);
}

export default {
  async fetch(request, env, ctx): Promise<Response> {
    try {
      return await handle(request, env, ctx);
    } catch {
      console.log(JSON.stringify({ event: "internal_error" }));
      return json({ error: "bad gateway" }, 502);
    }
  },
} satisfies ExportedHandler<Env>;
