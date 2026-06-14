import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as HttpClient from "effect/unstable/http/HttpClient";
import * as Cloudflare from "alchemy/Cloudflare";
import * as Drizzle from "alchemy/Drizzle";
import * as Neon from "alchemy/Neon";
import * as Test from "alchemy/Test/Vitest";
import Stack from "../../../../alchemy.run.ts";

// A live deploy talks to Cloudflare + Neon. Resolution is via the `env`
// auth method (see Cloudflare/Neon AuthProvider.ts): CLOUDFLARE_API_TOKEN
// (+ CLOUDFLARE_ACCOUNT_ID) and NEON_API_KEY. Without those we can't reach
// the cloud, so we register skipped placeholders to stay green locally.
const hasCreds = Boolean(
  process.env.CLOUDFLARE_API_TOKEN && process.env.CLOUDFLARE_ACCOUNT_ID && process.env.NEON_API_KEY,
);

// `Test.make` only builds the per-file API + a shared scope; it does not
// touch the cloud until `deploy`/`destroy` run, so calling it without creds
// is safe at import time.
const { test, beforeAll, afterAll, deploy, destroy } = Test.make({
  providers: Layer.mergeAll(Cloudflare.providers(), Drizzle.providers()).pipe(
    Layer.provideMerge(Neon.providers()),
  ),
  state: Cloudflare.state(),
});

if (hasCreds) {
  const stack = beforeAll(deploy(Stack));

  test(
    "deploys the Denora stack and serves a healthy server",
    Effect.gen(function* () {
      const out = yield* stack;

      expect(typeof out.serverUrl).toBe("string");
      expect(typeof out.webUrl).toBe("string");
      expect(out.databaseName).toBeTruthy();
      expect(out.hyperdriveId).toBeTruthy();
      expect(out.workosEventsWorkerName).toBeTruthy();

      const client = yield* HttpClient.HttpClient;

      const health = yield* client.get(`${out.serverUrl}/health`);
      expect(health.status).toBe(200);
      expect(yield* health.json).toEqual({ status: "ok" });

      const me = yield* client.get(`${out.serverUrl}/me`);
      expect(me.status).toBe(401);
    }),
  );

  // Only tear down in CI; locally we keep the stack so iteration is fast.
  afterAll.skipIf(!process.env.CI)(destroy(Stack));
} else {
  it.skip("alchemy stack deploy (set cloud creds to run)", () => {});
}
