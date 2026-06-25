import type { AssistantMessageEvent, Context as PiContext } from "@earendil-works/pi-ai";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { CloudflareAiGatewayModels } from "../../src/agent-loop/CloudflareAiGatewayModels.ts";
import { PiRuntime } from "../../src/agent-loop/PiRuntime.ts";
import { FakeAiGateway } from "../helpers/FakeAiGateway.ts";

const testModel = CloudflareAiGatewayModels.models["workers-ai/@cf/moonshotai/kimi-k2.6"].model;

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
