import { DurableObject } from "cloudflare:workers";

export const IP_LIMIT = 30;
export const IP_WINDOW_MS = 3_600_000;
export const CACHE_TTL_MS = 86_400_000;
export const MAX_CACHED_BYTES = 1_048_576;

export interface Admission {
  limited: boolean;
  pausedUntil: number;
  cached: string | null;
}

export async function sha256Hex(data: string | Uint8Array): Promise<string> {
  const bytes = typeof data === "string" ? new TextEncoder().encode(data) : data;
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

export class Gate extends DurableObject<Env> {
  private salt = new Uint8Array(0);

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(async () => {
      const sql = this.ctx.storage.sql;
      sql.exec("CREATE TABLE IF NOT EXISTS hits (ip TEXT NOT NULL, at INTEGER NOT NULL)");
      sql.exec("DROP INDEX IF EXISTS hits_ip");
      sql.exec("CREATE INDEX IF NOT EXISTS hits_ip_at ON hits (ip, at)");
      sql.exec("CREATE INDEX IF NOT EXISTS hits_at ON hits (at)");
      sql.exec(
        "CREATE TABLE IF NOT EXISTS state (id INTEGER PRIMARY KEY CHECK (id = 1), paused_until INTEGER NOT NULL)",
      );
      sql.exec("INSERT OR IGNORE INTO state (id, paused_until) VALUES (1, 0)");
      sql.exec("CREATE TABLE IF NOT EXISTS salt (id INTEGER PRIMARY KEY CHECK (id = 1), value BLOB NOT NULL)");
      sql.exec(
        "INSERT OR IGNORE INTO salt (id, value) VALUES (1, ?)",
        crypto.getRandomValues(new Uint8Array(32)).buffer,
      );
      this.salt = new Uint8Array(sql.exec<{ value: ArrayBuffer }>("SELECT value FROM salt WHERE id = 1").one().value);
      sql.exec("CREATE TABLE IF NOT EXISTS cache (key TEXT PRIMARY KEY, body TEXT NOT NULL, expires INTEGER NOT NULL)");
      sql.exec("CREATE INDEX IF NOT EXISTS cache_expires ON cache (expires)");
    });
  }

  async admit(ip: string, key: string, now: number): Promise<Admission> {
    const ipBytes = new TextEncoder().encode(ip);
    const salted = new Uint8Array(this.salt.length + ipBytes.length);
    salted.set(this.salt);
    salted.set(ipBytes, this.salt.length);
    const hashed = await sha256Hex(salted);

    const sql = this.ctx.storage.sql;
    sql.exec("DELETE FROM hits WHERE at <= ?", now - IP_WINDOW_MS);
    const { n } = sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM hits WHERE ip = ?", hashed).one();
    const limited = n >= IP_LIMIT;
    if (!limited) sql.exec("INSERT INTO hits (ip, at) VALUES (?, ?)", hashed, now);
    const { paused_until } = sql
      .exec<{ paused_until: number }>("SELECT paused_until FROM state WHERE id = 1")
      .one();
    const row = sql
      .exec<{ body: string }>("SELECT body FROM cache WHERE key = ? AND expires > ?", key, now)
      .toArray()[0];
    return { limited, pausedUntil: paused_until, cached: row?.body ?? null };
  }

  store(key: string, body: string, now: number): void {
    if (new TextEncoder().encode(body).byteLength > MAX_CACHED_BYTES) return;
    const sql = this.ctx.storage.sql;
    sql.exec("DELETE FROM cache WHERE expires <= ?", now);
    sql.exec("INSERT OR REPLACE INTO cache (key, body, expires) VALUES (?, ?, ?)", key, body, now + CACHE_TTL_MS);
  }

  pause(until: number): void {
    this.ctx.storage.sql.exec("UPDATE state SET paused_until = MAX(paused_until, ?) WHERE id = 1", until);
  }
}
