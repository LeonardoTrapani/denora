import type { AssistantMessageEvent, Context as PiContext, Model } from "@earendil-works/pi-ai";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { PiRuntime } from "../../src/agent-loop/PiRuntime.ts";
import { FakeAiGateway } from "../helpers/FakeAiGateway.ts";

const MODEL_ID = "@cf/meta/llama-3.1-8b-instruct-fast";

const testModel = {
  id: MODEL_ID,
  name: MODEL_ID,
  api: "openai-completions",
  provider: "cloudflare-workers-ai",
  baseUrl: "",
  reasoning: true,
  input: ["text", "image"],
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
  contextWindow: 128_000,
  maxTokens: 8192,
} satisfies Model<"openai-completions">;

const emptyContext = { messages: [] } satisfies PiContext;

const collectEvents = async (
  stream: AsyncIterable<AssistantMessageEvent>,
): Promise<AssistantMessageEvent[]> => {
  const events: AssistantMessageEvent[] = [];
  for await (const event of stream) events.push(event);
  return events;
};

describe("PiRuntime", () => {
  it.effect("keeps extracted streamFn usable after layer provisioning returns", () =>
    Effect.gen(function* () {
      const fake = FakeAiGateway.make(
        FakeAiGateway.sse(
          FakeAiGateway.json({ choices: [{ delta: { content: "stream ok" } }] }),
          FakeAiGateway.json({ choices: [{ finish_reason: "stop" }] }),
          FakeAiGateway.done(),
        ),
      );
      const layer = PiRuntime.layer.pipe(Layer.provide(FakeAiGateway.layer(fake)));

      const pi = yield* PiRuntime.Service.pipe(Effect.provide(layer));

      const events = yield* Effect.promise(async () =>
        collectEvents(await pi.streamFn(testModel, emptyContext)),
      );

      assert.deepStrictEqual(
        events.map((event) => event.type),
        ["start", "text_start", "text_delta", "text_end", "done"],
      );
    }),
  );
});
