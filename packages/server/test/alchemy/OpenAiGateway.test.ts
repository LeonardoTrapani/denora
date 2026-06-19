import { expect, it } from "@effect/vitest";
import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import * as Test from "alchemy/Test/Vitest";
import * as Effect from "effect/Effect";
import * as Schedule from "effect/Schedule";
import * as Stream from "effect/Stream";
import * as HttpClient from "effect/unstable/http/HttpClient";
import { AgentAiGateway } from "../../src/agent/AiGateway.ts";
import OpenAiGatewayWorker from "./fixtures/OpenAiGatewayWorker.ts";

const runsLiveGateway = process.env.DENORA_RUN_LIVE_AI_GATEWAY === "true";

const { test, beforeAll, afterAll, deploy, destroy } = Test.make({
  providers: Cloudflare.providers(),
  state: Cloudflare.state(),
});

const OpenAiGatewayStack = Alchemy.Stack(
  "DenoraOpenAiGatewayE2E",
  {
    providers: Cloudflare.providers(),
    state: Cloudflare.state(),
  },
  Effect.gen(function* () {
    const gateway = yield* AgentAiGateway.Gateway;
    const worker = yield* OpenAiGatewayWorker;

    return {
      gatewayId: gateway.gatewayId,
      url: worker.url.as<string>(),
    };
  }),
);

if (runsLiveGateway) {
  const stack = beforeAll(deploy(OpenAiGatewayStack));

  test(
    "builds an OpenAI provider URL through Cloudflare AI Gateway",
    Effect.gen(function* () {
      const out = yield* stack;
      const client = HttpClient.filterStatusOk(yield* HttpClient.HttpClient);
      const res = yield* client.get(`${out.url}/gateway-url`).pipe(retryEdgePropagation);

      expect(res.status).toBe(200);
      const body = (yield* res.json) as {
        gatewayId: string;
        provider: string;
        providerUrl: string;
      };

      expect(body.gatewayId).toBe(out.gatewayId);
      expect(body.provider).toBe("openai");
      expect(body.providerUrl).toContain(out.gatewayId);
      expect(body.providerUrl).toContain("openai");
      expect(body.providerUrl).toContain("gateway.ai.cloudflare.com");
    }),
    { timeout: 180_000 },
  );

  test(
    "generates text with OpenAI through Cloudflare AI Gateway",
    Effect.gen(function* () {
      const out = yield* stack;
      const client = HttpClient.filterStatusOk(yield* HttpClient.HttpClient);
      const res = yield* client
        .get(
          `${out.url}/generate?prompt=${encodeURIComponent(
            "Reply with the single word pong and no punctuation.",
          )}`,
        )
        .pipe(retryEdgePropagation);

      expect(res.status).toBe(200);
      const body = (yield* res.json) as {
        ok: boolean;
        text: string;
        finishReason: string;
        usage: { inputTokens?: number; outputTokens?: number };
      };

      expect(body.ok).toBe(true);
      expect(body.text.length).toBeGreaterThan(0);
      expect(body.finishReason).toBeTruthy();
      expect(body.usage.inputTokens).toBeGreaterThan(0);
      expect(body.usage.outputTokens).toBeGreaterThan(0);
    }),
    { timeout: 180_000 },
  );

  test(
    "streams text chunks with OpenAI through Cloudflare AI Gateway",
    Effect.gen(function* () {
      const out = yield* stack;
      const client = HttpClient.filterStatusOk(yield* HttpClient.HttpClient);
      const res = yield* client
        .get(
          `${out.url}/stream?prompt=${encodeURIComponent(
            "Write a short two-sentence greeting for a personal assistant app.",
          )}`,
        )
        .pipe(retryEdgePropagation);

      expect(res.status).toBe(200);

      const chunks = yield* res.stream.pipe(
        Stream.orDie,
        Stream.decodeText(),
        Stream.runCollect,
        Effect.map((chunk) => Array.from(chunk)),
      );
      const text = chunks.join("");

      expect(chunks.length).toBeGreaterThan(0);
      expect(text.length).toBeGreaterThan(0);
    }),
    { timeout: 180_000 },
  );

  afterAll.skipIf(!process.env.CI)(destroy(OpenAiGatewayStack));
} else {
  it.skip("alchemy OpenAI AI Gateway e2e (set DENORA_RUN_LIVE_AI_GATEWAY=true to run)", () => {});
}

const retryEdgePropagation = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.retry({
      schedule: Schedule.exponential("500 millis"),
      times: 10,
    }),
  );
