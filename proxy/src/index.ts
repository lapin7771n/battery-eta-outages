// Proxies the alerts.energy address directory for the Battery ETA app (plan Task 11).
// Until then every route answers 503, and the app falls back to the queue list.
export default {
  async fetch(): Promise<Response> {
    return Response.json({ error: "not ready" }, { status: 503 });
  },
} satisfies ExportedHandler;
