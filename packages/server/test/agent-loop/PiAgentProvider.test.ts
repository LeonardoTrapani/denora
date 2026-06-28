import { assert, describe, it } from "@effect/vitest";
import type { Model } from "@earendil-works/pi-ai";
import * as Effect from "effect/Effect";
import { PiAgentProvider } from "../../src/agent-loop/PiAgentProvider.ts";
import { FakeAiProvider } from "../helpers/FakeAiProvider.ts";

const collect = async <T>(iterable: AsyncIterable<T>): Promise<T[]> => {
  const values: T[] = [];
  for await (const value of iterable) values.push(value);
  return values;
};

describe("PiAgentProvider", () => {
  it("uses OpenRouter GPT-5.5 as the temporary default model", () => {
    assert.strictEqual(PiAgentProvider.defaultProviderId, "openrouter");
    assert.strictEqual(PiAgentProvider.defaultModelId, "openai/gpt-5.5");
    assert.strictEqual(PiAgentProvider.defaultModelSpecifier, "openrouter/openai/gpt-5.5");
    assert.strictEqual(PiAgentProvider.defaultModel.provider, "openrouter");
    assert.strictEqual(PiAgentProvider.defaultModel.id, "openai/gpt-5.5");
  });

  it.effect("delegates streaming through the configured provider layer", () => {
    const fake = FakeAiProvider.make(
      FakeAiProvider.sse(
        FakeAiProvider.json({ choices: [{ delta: { content: "hello" } }] }),
        FakeAiProvider.done(),
      ),
    );

    return Effect.gen(function* () {
      const service = yield* PiAgentProvider.Service;
      const stream = yield* service.stream({
        model: fake.defaultModel,
        context: { messages: [] },
      });
      const events = yield* Effect.promise(() => collect(stream));
      const result = yield* Effect.promise(() => stream.result());

      assert.strictEqual(fake.calls.length, 1);
      assert.strictEqual(fake.calls[0]?.model, fake.defaultModel.id);
      assert.deepStrictEqual(fake.calls[0]?.payload, { messages: [] });
      const lastEvent = events.at(-1);
      assert.strictEqual(lastEvent?.type, "done");
      if (lastEvent?.type !== "done") throw new Error("Expected done event.");
      assert.strictEqual(result, lastEvent.message);
    }).pipe(Effect.provide(FakeAiProvider.layer(fake)));
  });

  it.effect("applies layer-level option defaults", () => {
    const fake = FakeAiProvider.make();

    return Effect.gen(function* () {
      const service = yield* PiAgentProvider.Service;
      const stream = yield* service.stream({
        model: fake.defaultModel,
        context: { messages: [] },
        options: { temperature: 0.2 },
      });
      yield* Effect.promise(() => stream.result());

      assert.deepStrictEqual(fake.calls[0]?.options, { temperature: 0.2, maxTokens: 123 });
    }).pipe(Effect.provide(FakeAiProvider.layer(fake, { maxTokens: 123 })));
  });

  it.effect("can be supplied by a completely custom fake provider layer", () => {
    const customModel: Model<string> = {
      id: "custom-test-model",
      name: "Custom Test Model",
      api: "custom-test-api",
      provider: "custom-test-provider",
      baseUrl: "https://custom.test",
      reasoning: false,
      input: ["text"],
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
      contextWindow: 100,
      maxTokens: 100,
    };
    const fake = FakeAiProvider.make(undefined, { defaultModel: customModel });

    return Effect.gen(function* () {
      const service = yield* PiAgentProvider.Service;

      assert.strictEqual(service.defaultModel, customModel);
    }).pipe(Effect.provide(FakeAiProvider.layer(fake)));
  });
});
