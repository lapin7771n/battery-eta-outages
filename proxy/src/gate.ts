import { DurableObject } from "cloudflare:workers";

export const IP_LIMIT = 30;
export const IP_WINDOW_MS = 3_600_000;

export interface Admission {
  limited: boolean;
  pausedUntil: number;
}

export class Gate extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.blockConcurrencyWhile(async () => {
      this.ctx.storage.sql.exec(
        "CREATE TABLE IF NOT EXISTS hits (ip TEXT NOT NULL, at INTEGER NOT NULL)",
      );
      this.ctx.storage.sql.exec("CREATE INDEX IF NOT EXISTS hits_ip ON hits (ip)");
      this.ctx.storage.sql.exec(
        "CREATE TABLE IF NOT EXISTS state (id INTEGER PRIMARY KEY CHECK (id = 1), paused_until INTEGER NOT NULL)",
      );
      this.ctx.storage.sql.exec("INSERT OR IGNORE INTO state (id, paused_until) VALUES (1, 0)");
    });
  }

  admit(ip: string, now: number): Admission {
    const sql = this.ctx.storage.sql;
    sql.exec("DELETE FROM hits WHERE at <= ?", now - IP_WINDOW_MS);
    const { n } = sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM hits WHERE ip = ?", ip).one();
    const limited = n >= IP_LIMIT;
    if (!limited) sql.exec("INSERT INTO hits (ip, at) VALUES (?, ?)", ip, now);
    const { paused_until } = sql
      .exec<{ paused_until: number }>("SELECT paused_until FROM state WHERE id = 1")
      .one();
    return { limited, pausedUntil: paused_until };
  }

  pause(until: number): void {
    this.ctx.storage.sql.exec("UPDATE state SET paused_until = MAX(paused_until, ?) WHERE id = 1", until);
  }
}
