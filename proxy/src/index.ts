export { Gate } from "./gate";

const DISPATCH_URL =
  "https://api.github.com/repos/lapin7771n/battery-eta-outages/actions/workflows/outage-feed.yml/dispatches";

export type CronEnv = Env & { GITHUB_DISPATCH_TOKEN?: string };

// GitHub's own */10 schedule for outage-feed.yml never fired, so this cron starts the workflow instead.
export async function dispatchFeed(token: string | undefined): Promise<number | null> {
  if (!token) {
    console.log(JSON.stringify({ event: "dispatch_skipped" }));
    return null;
  }
  try {
    const reply = await fetch(DISPATCH_URL, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: "application/vnd.github+json",
        "Content-Type": "application/json",
        "User-Agent": "battery-eta-proxy",
        "X-GitHub-Api-Version": "2022-11-28",
      },
      body: JSON.stringify({ ref: "main" }),
      signal: AbortSignal.timeout(10_000),
    });
    console.log(JSON.stringify({ event: "dispatch", status: reply.status }));
    return reply.status;
  } catch {
    console.log(JSON.stringify({ event: "dispatch_failed" }));
    return 0;
  }
}

export default {
  async scheduled(_controller, env, ctx): Promise<void> {
    const cron: CronEnv = env;
    ctx.waitUntil(dispatchFeed(cron.GITHUB_DISPATCH_TOKEN));
  },

  async fetch(_request, _env, _ctx): Promise<Response> {
    return Response.json({ error: "gone" }, { status: 410 });
  },
} satisfies ExportedHandler<Env>;
