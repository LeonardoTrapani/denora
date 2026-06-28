import type {
  Api,
  AssistantMessageEvent,
  Context as PiContext,
  Model,
  SimpleStreamOptions,
} from "@earendil-works/pi-ai";
import { Type } from "@earendil-works/pi-ai";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { expect } from "vitest";
import { CloudflareAiGatewayModels } from "../../src/agent-loop/CloudflareAiGatewayModels.ts";
import { PiAgentModel } from "../../src/agent-loop/PiAgentModel.ts";
import { FakeAiGateway, type Fake } from "../helpers/FakeAiGateway.ts";

const MODEL_ID = "workers-ai/@cf/moonshotai/kimi-k2.6";
const SONNET_ID = "claude-sonnet-4-5";
const GPT_ID = "gpt-5.1";

const testModel = CloudflareAiGatewayModels.models[MODEL_ID].model;
const anthropicModel = CloudflareAiGatewayModels.models[SONNET_ID].model;
const gptModel = CloudflareAiGatewayModels.models[GPT_ID].model;

const emptyContext = { messages: [] } satisfies PiContext;

const contextWithWeatherTool = {
  messages: [],
  tools: [
    {
      name: "weather",
      description: "Get weather for a city.",
      parameters: Type.Object({ city: Type.String() }),
    },
  ],
} satisfies PiContext;

const collectEvents = async (
  stream: AsyncIterable<AssistantMessageEvent>,
): Promise<AssistantMessageEvent[]> => {
  const events: AssistantMessageEvent[] = [];
  for await (const event of stream) events.push(event);
  return events;
};

const streamWithFake = (
  fake: Fake,
  input: {
    readonly model?: Model<Api> | undefined;
    readonly context?: PiContext | undefined;
    readonly options?: SimpleStreamOptions | undefined;
  } = {},
): Effect.Effect<AssistantMessageEvent[]> =>
  Effect.gen(function* () {
    const service = yield* PiAgentModel.Service;
    const stream = yield* service.stream({
      model: input.model ?? testModel,
      context: input.context ?? emptyContext,
      options: input.options,
    });
    return yield* Effect.promise(() => collectEvents(stream));
  }).pipe(Effect.provide(FakeAiGateway.layer(fake)));

const finish = (finishReason = "stop") =>
  FakeAiGateway.json({ choices: [{ finish_reason: finishReason }] });

describe("PiAgentModel Cloudflare AI Gateway adapter", () => {
  it("exposes Cloudflare AI Gateway catalog defaults and route model ids", () => {
    assert.strictEqual(CloudflareAiGatewayModels.defaultModelId, SONNET_ID);
    assert.strictEqual(CloudflareAiGatewayModels.defaultModel, anthropicModel);
    assert.strictEqual(CloudflareAiGatewayModels.modelFor(SONNET_ID)?.id, SONNET_ID);
    assert.isAtLeast(CloudflareAiGatewayModels.list().length, 60);
    assert.isAtLeast(CloudflareAiGatewayModels.modelsByDisplayProvider("anthropic").length, 7);
    assert.isAtLeast(CloudflareAiGatewayModels.modelsByDisplayProvider("openai").length, 15);
    assert.isAtLeast(CloudflareAiGatewayModels.modelsByDisplayProvider("moonshotai").length, 3);
    assert.isAtLeast(CloudflareAiGatewayModels.modelsByDisplayProvider("zai").length, 2);
    assert.isAtLeast(CloudflareAiGatewayModels.modelsByFamily("llama").length, 10);

    const ids = new Set<string>();
    for (const [id, entry] of Object.entries(CloudflareAiGatewayModels.models)) {
      assert.strictEqual(entry.model.id, id);
      assert.isFalse(ids.has(id), `Duplicate model id ${id}`);
      ids.add(id);
      assert.isString(entry.catalog.displayProvider.id);
      assert.isString(entry.catalog.displayProvider.name);
      assert.strictEqual(entry.catalog.modelTask, "text-generation");
      assert.deepEqual(entry.catalog.modalities.output, ["text"]);
    }
    assert.strictEqual(CloudflareAiGatewayModels.models[SONNET_ID].route.model, SONNET_ID);
    assert.strictEqual(CloudflareAiGatewayModels.models[GPT_ID].route.model, GPT_ID);
    assert.strictEqual(
      CloudflareAiGatewayModels.models[MODEL_ID].route.model,
      "@cf/moonshotai/kimi-k2.6",
    );
  });

  it("exposes frontend-safe catalog providers grouped by display provider", () => {
    const catalog = CloudflareAiGatewayModels.catalogResponse();
    assert.strictEqual(catalog.defaultModelId, SONNET_ID);
    assert.isDefined(CloudflareAiGatewayModels.find(catalog.defaultModelId));
    assert.isAtLeast(catalog.providers.length, 8);
    assert.deepEqual(
      catalog.providers.slice(0, 5).map((provider) => provider.id),
      ["anthropic", "openai", "moonshotai", "zai", "meta"],
    );

    const flattened = catalog.providers.flatMap((provider) => provider.models);
    assert.lengthOf(flattened, CloudflareAiGatewayModels.list().length);
    assert.isTrue(flattened.some((item) => item.default));
    assert.isTrue(flattened.every((item) => item.outputModalities.includes("text")));
    assert.isFalse(
      flattened.some((item) =>
        CloudflareAiGatewayModels.nonChatCatalogCategories.includes(item.family as never),
      ),
    );
    assert.isUndefined((flattened[0] as unknown as Record<string, unknown>).route);
  });

  it.effect("routes Kimi through the Cloudflare AI Gateway Workers AI provider", () =>
    Effect.gen(function* () {
      const fake = FakeAiGateway.make(FakeAiGateway.sse(finish(), FakeAiGateway.done()), {
        id: "production-gateway",
      });

      const events = yield* streamWithFake(fake);

      assert.strictEqual(fake.calls.length, 0);
      assert.strictEqual(fake.gatewayCalls.length, 1);
      expect(fake.gatewayCalls[0]).toMatchObject({
        request: {
          provider: "workers-ai",
          endpoint: "@cf/moonshotai/kimi-k2.6",
          headers: { "content-type": "application/json" },
          query: {
            messages: [],
            stream: true,
            stream_options: { include_usage: true },
          },
        },
        options: {
          gateway: { id: "production-gateway" },
        },
      });
      expect(events.at(-1)).toMatchObject({ type: "done", reason: "stop" });
    }),
  );

  it.effect("does not send unsupported Workers AI reasoning_effort", () =>
    Effect.gen(function* () {
      const fake = FakeAiGateway.make(FakeAiGateway.sse(finish(), FakeAiGateway.done()));

      yield* streamWithFake(fake, { options: { reasoning: "high" } });

      const request = fake.gatewayCalls[0]?.request as { readonly query: Record<string, unknown> };
      assert.notProperty(request.query, "reasoning_effort");
    }),
  );

  it.effect("forwards session affinity as an AI Gateway extra header", () =>
    Effect.gen(function* () {
      const fake = FakeAiGateway.make(FakeAiGateway.sse(finish()));

      yield* streamWithFake(fake, { options: { sessionId: "session-123" } });

      expect(fake.gatewayCalls[0]?.options).toMatchObject({
        extraHeaders: { "x-session-affinity": "session-123" },
      });
    }),
  );

  it.effect("routes Anthropic models through provider AI Gateway", () =>
    Effect.gen(function* () {
      const fake = FakeAiGateway.make(
        FakeAiGateway.sse(
          anthropicEvent("message_start", {
            type: "message_start",
            message: {
              id: "msg_123",
              model: SONNET_ID,
              usage: {
                input_tokens: 10,
                output_tokens: 0,
                cache_read_input_tokens: 3,
              },
            },
          }),
          anthropicEvent("content_block_start", {
            type: "content_block_start",
            index: 0,
            content_block: { type: "text", text: "" },
          }),
          anthropicEvent("content_block_delta", {
            type: "content_block_delta",
            index: 0,
            delta: { type: "text_delta", text: "hello" },
          }),
          anthropicEvent("content_block_stop", { type: "content_block_stop", index: 0 }),
          anthropicEvent("message_delta", {
            type: "message_delta",
            delta: { stop_reason: "end_turn" },
            usage: { output_tokens: 2 },
          }),
          anthropicEvent("message_stop", { type: "message_stop" }),
        ),
        { id: "production-gateway" },
      );

      const events = yield* streamWithFake(fake, {
        model: anthropicModel,
        context: {
          systemPrompt: "You are Denora.",
          messages: [{ role: "user", content: "Say hello.", timestamp: 1 }],
          tools: contextWithWeatherTool.tools,
        },
        options: { maxTokens: 123, temperature: 0.2, sessionId: "session-123" },
      });

      assert.strictEqual(fake.calls.length, 0);
      assert.strictEqual(fake.gatewayCalls.length, 1);
      expect(fake.gatewayCalls[0]).toMatchObject({
        request: {
          provider: "anthropic",
          endpoint: "v1/messages",
          headers: {
            "content-type": "application/json",
            "anthropic-version": "2023-06-01",
          },
          query: {
            model: SONNET_ID,
            messages: [{ role: "user", content: "Say hello." }],
            system: "You are Denora.",
            max_tokens: 123,
            temperature: 0.2,
            stream: true,
            tools: [
              {
                name: "weather",
                description: "Get weather for a city.",
                input_schema: {
                  type: "object",
                  properties: { city: { type: "string" } },
                  required: ["city"],
                },
              },
            ],
          },
        },
        options: {
          gateway: { id: "production-gateway" },
          extraHeaders: { "x-session-affinity": "session-123" },
        },
      });
      expect(events.at(-1)).toMatchObject({
        type: "done",
        reason: "stop",
        message: {
          responseId: "msg_123",
          content: [{ type: "text", text: "hello" }],
          usage: { input: 7, output: 2, cacheRead: 3, totalTokens: 12 },
        },
      });
    }),
  );

  it.effect("sends Anthropic extended thinking payload when reasoning is requested", () =>
    Effect.gen(function* () {
      const fake = FakeAiGateway.make(
        FakeAiGateway.sse(
          anthropicEvent("message_delta", {
            type: "message_delta",
            delta: { stop_reason: "end_turn" },
          }),
          anthropicEvent("message_stop", { type: "message_stop" }),
        ),
      );

      yield* streamWithFake(fake, {
        model: anthropicModel,
        options: { reasoning: "medium", maxTokens: 8_192, temperature: 0.2 },
      });

      const request = fake.gatewayCalls[0]?.request as { readonly query: Record<string, unknown> };
      expect(request).toMatchObject({
        provider: "anthropic",
        endpoint: "v1/messages",
        query: {
          model: SONNET_ID,
          max_tokens: 16_384,
          stream: true,
          thinking: { type: "enabled", budget_tokens: 8_192, display: "summarized" },
        },
      });
      const query = request.query;
      assert.notProperty(query, "reasoning");
      assert.notProperty(query, "reasoning_effort");
      assert.notProperty(query, "temperature");
    }),
  );

  it.effect("sends Anthropic adaptive thinking payload for adaptive Claude models", () =>
    Effect.gen(function* () {
      const fake = FakeAiGateway.make(
        FakeAiGateway.sse(
          anthropicEvent("message_delta", {
            type: "message_delta",
            delta: { stop_reason: "end_turn" },
          }),
          anthropicEvent("message_stop", { type: "message_stop" }),
        ),
      );
      const adaptiveModel = CloudflareAiGatewayModels.models["claude-sonnet-4-6"].model;

      yield* streamWithFake(fake, {
        model: adaptiveModel,
        options: { reasoning: "xhigh", maxTokens: 8_192 },
      });

      const request = fake.gatewayCalls[0]?.request as { readonly query: Record<string, unknown> };
      expect(request.query).toMatchObject({
        model: "claude-sonnet-4-6",
        thinking: { type: "adaptive", display: "summarized" },
        output_config: { effort: "max" },
      });
    }),
  );

  it.effect("routes GPT models through provider AI Gateway Responses", () =>
    Effect.gen(function* () {
      const fake = FakeAiGateway.make(
        FakeAiGateway.sse(
          openAiEvent({ type: "response.created", response: { id: "resp_123" } }),
          openAiEvent({
            type: "response.output_item.added",
            item: { type: "message", id: "msg_123" },
          }),
          openAiEvent({ type: "response.output_text.delta", delta: "hello" }),
          openAiEvent({
            type: "response.output_item.done",
            item: {
              type: "message",
              id: "msg_123",
              content: [{ type: "output_text", text: "hello", annotations: [] }],
            },
          }),
          openAiEvent({
            type: "response.completed",
            response: {
              id: "resp_123",
              model: GPT_ID,
              status: "completed",
              usage: {
                input_tokens: 12,
                output_tokens: 2,
                total_tokens: 14,
                input_tokens_details: { cached_tokens: 5 },
              },
            },
          }),
        ),
        { id: "production-gateway" },
      );

      const events = yield* streamWithFake(fake, {
        model: gptModel,
        context: {
          systemPrompt: "You are Denora.",
          messages: [{ role: "user", content: "Say hello.", timestamp: 1 }],
          tools: contextWithWeatherTool.tools,
        },
        options: { maxTokens: 456, temperature: 0.3, sessionId: "session-456" },
      });

      assert.strictEqual(fake.calls.length, 0);
      assert.strictEqual(fake.gatewayCalls.length, 1);
      expect(fake.gatewayCalls[0]).toMatchObject({
        request: {
          provider: "openai",
          endpoint: "v1/responses",
          headers: { "content-type": "application/json" },
          query: {
            model: GPT_ID,
            input: [
              { role: "developer", content: "You are Denora." },
              { role: "user", content: "Say hello." },
            ],
            stream: true,
            store: false,
            max_output_tokens: 456,
            temperature: 0.3,
            tools: [
              {
                type: "function",
                name: "weather",
                description: "Get weather for a city.",
                parameters: {
                  type: "object",
                  properties: { city: { type: "string" } },
                  required: ["city"],
                },
                strict: false,
              },
            ],
          },
        },
        options: {
          gateway: { id: "production-gateway" },
          extraHeaders: { "x-session-affinity": "session-456" },
        },
      });
      expect(events.at(-1)).toMatchObject({
        type: "done",
        reason: "stop",
        message: {
          responseId: "resp_123",
          content: [{ type: "text", text: "hello" }],
          usage: { input: 7, output: 2, cacheRead: 5, totalTokens: 14 },
        },
      });
    }),
  );

  it.effect("applies model parameters and request hooks", () =>
    Effect.gen(function* () {
      const fake = FakeAiGateway.make(FakeAiGateway.sse(finish()));
      const payloads: unknown[] = [];
      const responses: unknown[] = [];

      yield* streamWithFake(fake, {
        options: {
          maxTokens: 123,
          temperature: 0.2,
          onPayload: (payload) => {
            payloads.push(payload);
          },
          onResponse: (response) => {
            responses.push(response);
          },
        },
      });

      expect(fake.gatewayCalls[0]?.request).toMatchObject({
        query: { max_tokens: 123, temperature: 0.2 },
      });
      expect(payloads[0]).toMatchObject({ max_tokens: 123, temperature: 0.2 });
      expect(responses[0]).toMatchObject({ status: 200 });
    }),
  );

  it.effect("translates delayed text deltas before the stream completes", () =>
    Effect.gen(function* () {
      const fake = FakeAiGateway.make(
        FakeAiGateway.sse(
          FakeAiGateway.json({ choices: [{ delta: { content: "hello " } }] }),
          FakeAiGateway.json({ choices: [{ delta: { content: "world" } }] }, 25),
          finish(),
        ),
      );

      yield* Effect.gen(function* () {
        const service = yield* PiAgentModel.Service;
        const stream = yield* service.stream({ model: testModel, context: emptyContext });
        yield* Effect.promise(async () => {
          const iterator = stream[Symbol.asyncIterator]();
          const start = await readEvent(iterator);
          const textStart = await readEvent(iterator);
          const firstDelta = await readEvent(iterator);
          const timeout = Symbol("timeout");
          const pendingNext = readEvent(iterator);
          const early = await Promise.race([
            pendingNext.then((event) => ({ type: "event" as const, event })),
            new Promise<typeof timeout>((resolve) => setTimeout(() => resolve(timeout), 5)),
          ]);
          const secondDelta = early === timeout ? await pendingNext : early.event;
          const remaining: AssistantMessageEvent[] = [];
          for await (const event of stream) remaining.push(event);

          expect(start).toMatchObject({ type: "start" });
          expect(textStart).toMatchObject({ type: "text_start", contentIndex: 0 });
          expect(firstDelta).toMatchObject({
            type: "text_delta",
            contentIndex: 0,
            delta: "hello ",
          });
          assert.strictEqual(early, timeout);
          expect(secondDelta).toMatchObject({
            type: "text_delta",
            contentIndex: 0,
            delta: "world",
          });
          expect(remaining.at(-1)).toMatchObject({
            type: "done",
            message: { content: [{ type: "text", text: "hello world" }] },
          });
        });
      }).pipe(Effect.provide(FakeAiGateway.layer(fake)));
    }),
  );

  it.effect("joins multi-line SSE data payloads", () =>
    Effect.gen(function* () {
      const fake = FakeAiGateway.make(
        FakeAiGateway.sse(
          FakeAiGateway.raw('data: {"choices":[{"delta":\ndata: {"content":"hello"}}]}\n\n'),
          finish(),
        ),
      );

      const events = yield* streamWithFake(fake);

      expect(events).toContainEqual(
        expect.objectContaining({ type: "text_delta", contentIndex: 0, delta: "hello" }),
      );
      expect(events.at(-1)).toMatchObject({ type: "done", reason: "stop" });
    }),
  );

  it.effect("parses SSE events split across byte chunks", () =>
    Effect.gen(function* () {
      const fake = FakeAiGateway.make(
        FakeAiGateway.sse(
          FakeAiGateway.raw('data: {"choices":[{"delta":{"content":"hel'),
          FakeAiGateway.raw('lo"}}]}\n\n'),
          finish(),
        ),
      );

      const events = yield* streamWithFake(fake);

      expect(events).toContainEqual(
        expect.objectContaining({ type: "text_delta", contentIndex: 0, delta: "hello" }),
      );
      expect(events.at(-1)).toMatchObject({ type: "done", reason: "stop" });
    }),
  );

  it.effect("parses CRLF-delimited SSE events", () =>
    Effect.gen(function* () {
      const fake = FakeAiGateway.make(
        FakeAiGateway.sse(
          FakeAiGateway.raw('data: {"choices":[{"delta":{"content":"hello"}}]}\r\n\r\n'),
          finish(),
        ),
      );

      const events = yield* streamWithFake(fake);

      expect(events).toContainEqual(
        expect.objectContaining({ type: "text_delta", contentIndex: 0, delta: "hello" }),
      );
      expect(events.at(-1)).toMatchObject({ type: "done", reason: "stop" });
    }),
  );

  it.effect("emits a terminal error for invalid SSE JSON", () =>
    Effect.gen(function* () {
      const fake = FakeAiGateway.make(FakeAiGateway.sse(FakeAiGateway.raw("data: not-json\n\n")));

      const events = yield* streamWithFake(fake);

      expect(events).toEqual([
        expect.objectContaining({ type: "start" }),
        expect.objectContaining({
          type: "error",
          reason: "error",
          error: expect.objectContaining({ stopReason: "error" }),
        }),
      ]);
    }),
  );

  it.effect("emits a terminal error for schema-invalid stream chunks", () =>
    Effect.gen(function* () {
      const fake = FakeAiGateway.make(
        FakeAiGateway.sse(FakeAiGateway.json({ choices: [{ delta: { content: 123 } }] })),
      );

      const events = yield* streamWithFake(fake);

      expect(events).toEqual([
        expect.objectContaining({ type: "start" }),
        expect.objectContaining({
          type: "error",
          reason: "error",
          error: expect.objectContaining({ stopReason: "error" }),
        }),
      ]);
    }),
  );

  it.effect("emits a terminal error instead of hanging when the response body is locked", () =>
    Effect.gen(function* () {
      const response = new Response(new ReadableStream<Uint8Array>({ start() {} }), {
        headers: { "content-type": "text/event-stream" },
      });
      const body = response.body;
      if (body === null) throw new Error("Expected test response body.");
      const reader = body.getReader();
      const fake = FakeAiGateway.make(FakeAiGateway.response(response));

      yield* Effect.gen(function* () {
        const service = yield* PiAgentModel.Service;
        const stream = yield* service.stream({ model: testModel, context: emptyContext });
        const events = yield* Effect.promise(async () => {
          try {
            const timeout = Symbol("timeout");
            const result = await Promise.race([
              collectEvents(stream),
              new Promise<typeof timeout>((resolve) => setTimeout(() => resolve(timeout), 1_000)),
            ]);
            if (result === timeout) {
              throw new Error("Timed out waiting for locked response body failure.");
            }
            return result;
          } finally {
            reader.releaseLock();
            await body.cancel().catch(() => undefined);
          }
        });

        expect(events).toEqual([
          expect.objectContaining({ type: "start" }),
          expect.objectContaining({
            type: "error",
            reason: "error",
            error: expect.objectContaining({
              stopReason: "error",
              errorMessage: expect.stringMatching(/locked/i),
            }),
          }),
        ]);
      }).pipe(Effect.provide(FakeAiGateway.layer(fake)));
    }),
  );

  it.effect("translates reasoning deltas to thinking content", () =>
    Effect.gen(function* () {
      const fake = FakeAiGateway.make(
        FakeAiGateway.sse(
          FakeAiGateway.json({ choices: [{ delta: { reasoning_content: "inspect " } }] }),
          FakeAiGateway.json({ choices: [{ delta: { reasoning_content: "inputs" } }] }),
          finish(),
        ),
      );

      const events = yield* streamWithFake(fake);

      expect(events).toEqual([
        expect.objectContaining({ type: "start" }),
        expect.objectContaining({ type: "thinking_start", contentIndex: 0 }),
        expect.objectContaining({
          type: "thinking_delta",
          contentIndex: 0,
          delta: "inspect ",
        }),
        expect.objectContaining({ type: "thinking_delta", contentIndex: 0, delta: "inputs" }),
        expect.objectContaining({
          type: "thinking_end",
          contentIndex: 0,
          content: "inspect inputs",
        }),
        expect.objectContaining({ type: "done", reason: "stop" }),
      ]);
      expect(events.at(-1)).toMatchObject({
        type: "done",
        message: {
          content: [
            {
              type: "thinking",
              thinking: "inspect inputs",
              thinkingSignature: "reasoning_content",
            },
          ],
        },
      });
    }),
  );

  it.effect("parses Anthropic thinking and signature deltas", () =>
    Effect.gen(function* () {
      const fake = FakeAiGateway.make(
        FakeAiGateway.sse(
          anthropicEvent("message_start", {
            type: "message_start",
            message: { id: "msg_thinking", model: SONNET_ID },
          }),
          anthropicEvent("content_block_start", {
            type: "content_block_start",
            index: 0,
            content_block: { type: "thinking", thinking: "" },
          }),
          anthropicEvent("content_block_delta", {
            type: "content_block_delta",
            index: 0,
            delta: { type: "thinking_delta", thinking: "inspect " },
          }),
          anthropicEvent("content_block_delta", {
            type: "content_block_delta",
            index: 0,
            delta: { type: "thinking_delta", thinking: "inputs" },
          }),
          anthropicEvent("content_block_delta", {
            type: "content_block_delta",
            index: 0,
            delta: { type: "signature_delta", signature: "sig_" },
          }),
          anthropicEvent("content_block_delta", {
            type: "content_block_delta",
            index: 0,
            delta: { type: "signature_delta", signature: "123" },
          }),
          anthropicEvent("content_block_stop", { type: "content_block_stop", index: 0 }),
          anthropicEvent("content_block_start", {
            type: "content_block_start",
            index: 1,
            content_block: { type: "text", text: "" },
          }),
          anthropicEvent("content_block_delta", {
            type: "content_block_delta",
            index: 1,
            delta: { type: "text_delta", text: "done" },
          }),
          anthropicEvent("content_block_stop", { type: "content_block_stop", index: 1 }),
          anthropicEvent("message_delta", {
            type: "message_delta",
            delta: { stop_reason: "end_turn" },
          }),
          anthropicEvent("message_stop", { type: "message_stop" }),
        ),
      );

      const events = yield* streamWithFake(fake, { model: anthropicModel });

      expect(events).toEqual([
        expect.objectContaining({ type: "start" }),
        expect.objectContaining({ type: "thinking_start", contentIndex: 0 }),
        expect.objectContaining({ type: "thinking_delta", contentIndex: 0, delta: "inspect " }),
        expect.objectContaining({ type: "thinking_delta", contentIndex: 0, delta: "inputs" }),
        expect.objectContaining({
          type: "thinking_end",
          contentIndex: 0,
          content: "inspect inputs",
        }),
        expect.objectContaining({ type: "text_start", contentIndex: 1 }),
        expect.objectContaining({ type: "text_delta", contentIndex: 1, delta: "done" }),
        expect.objectContaining({ type: "text_end", contentIndex: 1, content: "done" }),
        expect.objectContaining({ type: "done", reason: "stop" }),
      ]);
      expect(events.at(-1)).toMatchObject({
        type: "done",
        message: {
          content: [
            { type: "thinking", thinking: "inspect inputs", thinkingSignature: "sig_123" },
            { type: "text", text: "done" },
          ],
        },
      });
    }),
  );

  it.effect("streams native Workers AI response text when tools are not requested", () =>
    Effect.gen(function* () {
      const fake = FakeAiGateway.make(
        FakeAiGateway.sse(
          FakeAiGateway.json({ response: "hello " }),
          FakeAiGateway.json({ response: "native" }),
          FakeAiGateway.done(),
        ),
      );

      const events = yield* streamWithFake(fake);

      expect(events).toEqual([
        expect.objectContaining({ type: "start" }),
        expect.objectContaining({ type: "text_start", contentIndex: 0 }),
        expect.objectContaining({ type: "text_delta", contentIndex: 0, delta: "hello " }),
        expect.objectContaining({ type: "text_delta", contentIndex: 0, delta: "native" }),
        expect.objectContaining({ type: "text_end", contentIndex: 0, content: "hello native" }),
        expect.objectContaining({ type: "done", reason: "stop" }),
      ]);
    }),
  );

  it.effect("does not duplicate text when chunks include native response and OpenAI delta", () =>
    Effect.gen(function* () {
      const fake = FakeAiGateway.make(
        FakeAiGateway.sse(
          FakeAiGateway.json({
            response: "DEN",
            choices: [{ delta: { content: "DEN" } }],
          }),
          FakeAiGateway.json({
            response: "ORA",
            choices: [{ delta: { content: "ORA" } }],
          }),
          finish(),
        ),
      );

      const events = yield* streamWithFake(fake);

      expect(events.at(-1)).toMatchObject({
        type: "done",
        message: { content: [{ type: "text", text: "DENORA" }] },
      });
      expect(events.filter((event) => event.type === "text_delta")).toEqual([
        expect.objectContaining({ type: "text_delta", delta: "DEN" }),
        expect.objectContaining({ type: "text_delta", delta: "ORA" }),
      ]);
    }),
  );

  it.effect("assembles streamed tool-call arguments", () =>
    Effect.gen(function* () {
      const fake = FakeAiGateway.make(
        FakeAiGateway.sse(
          FakeAiGateway.json({
            choices: [
              {
                delta: {
                  tool_calls: [
                    {
                      index: 0,
                      id: "call_weather",
                      function: { name: "weather", arguments: '{"city":"San' },
                    },
                  ],
                },
              },
            ],
          }),
          FakeAiGateway.json({
            choices: [
              {
                delta: {
                  tool_calls: [{ index: 0, function: { arguments: ' Francisco"}' } }],
                },
              },
            ],
          }),
          finish("tool_calls"),
        ),
      );

      const events = yield* streamWithFake(fake);

      expect(events).toEqual([
        expect.objectContaining({ type: "start" }),
        expect.objectContaining({ type: "toolcall_start", contentIndex: 0 }),
        expect.objectContaining({ type: "toolcall_delta", contentIndex: 0, delta: '{"city":"San' }),
        expect.objectContaining({ type: "toolcall_delta", contentIndex: 0, delta: ' Francisco"}' }),
        expect.objectContaining({
          type: "toolcall_end",
          contentIndex: 0,
          toolCall: {
            type: "toolCall",
            id: "call_weather",
            name: "weather",
            arguments: { city: "San Francisco" },
          },
        }),
        expect.objectContaining({ type: "done", reason: "toolUse" }),
      ]);
    }),
  );

  it.effect("routes interleaved parallel tool-call chunks by stream index", () =>
    Effect.gen(function* () {
      const fake = FakeAiGateway.make(
        FakeAiGateway.sse(
          FakeAiGateway.json({
            choices: [
              {
                delta: {
                  tool_calls: [
                    { index: 0, id: "call_a", function: { name: "weather", arguments: "" } },
                  ],
                },
              },
            ],
          }),
          FakeAiGateway.json({
            choices: [
              {
                delta: {
                  tool_calls: [
                    { index: 1, id: "call_b", function: { name: "time", arguments: "" } },
                  ],
                },
              },
            ],
          }),
          FakeAiGateway.json({
            choices: [
              {
                delta: {
                  tool_calls: [{ index: 0, function: { arguments: '{"city":"Paris"}' } }],
                },
              },
            ],
          }),
          FakeAiGateway.json({
            choices: [
              {
                delta: {
                  tool_calls: [{ index: 1, function: { arguments: '{"zone":"CET"}' } }],
                },
              },
            ],
          }),
          finish("tool_calls"),
        ),
      );

      const events = yield* streamWithFake(fake);

      expect(events).toEqual([
        expect.objectContaining({ type: "start" }),
        expect.objectContaining({ type: "toolcall_start", contentIndex: 0 }),
        expect.objectContaining({ type: "toolcall_delta", contentIndex: 0, delta: "" }),
        expect.objectContaining({ type: "toolcall_start", contentIndex: 1 }),
        expect.objectContaining({ type: "toolcall_delta", contentIndex: 1, delta: "" }),
        expect.objectContaining({
          type: "toolcall_delta",
          contentIndex: 0,
          delta: '{"city":"Paris"}',
        }),
        expect.objectContaining({
          type: "toolcall_delta",
          contentIndex: 1,
          delta: '{"zone":"CET"}',
        }),
        expect.objectContaining({
          type: "toolcall_end",
          contentIndex: 0,
          toolCall: {
            type: "toolCall",
            id: "call_a",
            name: "weather",
            arguments: { city: "Paris" },
          },
        }),
        expect.objectContaining({
          type: "toolcall_end",
          contentIndex: 1,
          toolCall: {
            type: "toolCall",
            id: "call_b",
            name: "time",
            arguments: { zone: "CET" },
          },
        }),
        expect.objectContaining({ type: "done", reason: "toolUse" }),
      ]);
    }),
  );

  it.effect("decodes native Workers AI buffered tool JSON when tools are requested", () =>
    Effect.gen(function* () {
      const fake = FakeAiGateway.make(
        FakeAiGateway.sse(
          FakeAiGateway.json({ response: '{"name":"weather",' }),
          FakeAiGateway.json({ response: '"parameters":{"city":"Paris"}}' }),
          FakeAiGateway.done(),
        ),
      );

      const events = yield* streamWithFake(fake, { context: contextWithWeatherTool });

      expect(events).toEqual([
        expect.objectContaining({ type: "start" }),
        expect.objectContaining({ type: "toolcall_start", contentIndex: 0 }),
        expect.objectContaining({
          type: "toolcall_delta",
          contentIndex: 0,
          delta: '{"city":"Paris"}',
        }),
        expect.objectContaining({
          type: "toolcall_end",
          contentIndex: 0,
          toolCall: {
            type: "toolCall",
            id: expect.any(String),
            name: "weather",
            arguments: { city: "Paris" },
          },
        }),
        expect.objectContaining({ type: "done", reason: "toolUse" }),
      ]);
    }),
  );

  it.effect("falls back to native response text when buffered tool JSON is prose", () =>
    Effect.gen(function* () {
      const fake = FakeAiGateway.make(
        FakeAiGateway.sse(
          FakeAiGateway.json({ response: "I can answer without a tool." }),
          FakeAiGateway.done(),
        ),
      );

      const events = yield* streamWithFake(fake, { context: contextWithWeatherTool });

      expect(events.at(-1)).toMatchObject({
        type: "done",
        reason: "stop",
        message: { content: [{ type: "text", text: "I can answer without a tool." }] },
      });
    }),
  );

  it.effect("decodes top-level native tool_calls", () =>
    Effect.gen(function* () {
      const fake = FakeAiGateway.make(
        FakeAiGateway.sse(
          FakeAiGateway.json({
            tool_calls: [
              {
                id: "call_native",
                name: "weather",
                arguments: { city: "Rome" },
              },
            ],
          }),
          FakeAiGateway.done(),
        ),
      );

      const events = yield* streamWithFake(fake, { context: contextWithWeatherTool });

      expect(events).toEqual([
        expect.objectContaining({ type: "start" }),
        expect.objectContaining({ type: "toolcall_start", contentIndex: 0 }),
        expect.objectContaining({
          type: "toolcall_delta",
          contentIndex: 0,
          delta: '{"city":"Rome"}',
        }),
        expect.objectContaining({
          type: "toolcall_end",
          contentIndex: 0,
          toolCall: {
            type: "toolCall",
            id: "call_native",
            name: "weather",
            arguments: { city: "Rome" },
          },
        }),
        expect.objectContaining({ type: "done", reason: "toolUse" }),
      ]);
    }),
  );

  it.effect("reports streamed token usage and cache reads", () =>
    Effect.gen(function* () {
      const fake = FakeAiGateway.make(
        FakeAiGateway.sse(
          FakeAiGateway.json({
            choices: [],
            usage: {
              prompt_tokens: 10,
              completion_tokens: 4,
              total_tokens: 14,
              prompt_tokens_details: { cached_tokens: 3 },
            },
          }),
          FakeAiGateway.json({
            usage: {
              prompt_tokens: 0,
              completion_tokens: 0,
              total_tokens: 0,
            },
          }),
          finish(),
        ),
      );

      const events = yield* streamWithFake(fake);

      expect(events.at(-1)).toMatchObject({
        type: "done",
        message: {
          usage: {
            input: 7,
            output: 4,
            cacheRead: 3,
            cacheWrite: 0,
            totalTokens: 14,
            cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
          },
        },
      });
    }),
  );

  it.effect("maps input/output token usage and cache writes", () =>
    Effect.gen(function* () {
      const fake = FakeAiGateway.make(
        FakeAiGateway.sse(
          FakeAiGateway.json({
            usage: {
              input_tokens: 20,
              output_tokens: 5,
              cache_read_tokens: 4,
              cache_write_tokens: 3,
            },
          }),
          finish(),
        ),
      );

      const events = yield* streamWithFake(fake);

      expect(events.at(-1)).toMatchObject({
        type: "done",
        message: {
          usage: {
            input: 13,
            output: 5,
            cacheRead: 4,
            cacheWrite: 3,
            totalTokens: 25,
          },
        },
      });
    }),
  );

  it.effect("maps clean DONE without an explicit finish reason to stop", () =>
    Effect.gen(function* () {
      const fake = FakeAiGateway.make(FakeAiGateway.sse(FakeAiGateway.done()));

      const events = yield* streamWithFake(fake);

      expect(events).toEqual([
        expect.objectContaining({ type: "start" }),
        expect.objectContaining({ type: "done", reason: "stop" }),
      ]);
    }),
  );

  it.effect("keeps toolUse when a tool call finishes with stop", () =>
    Effect.gen(function* () {
      const fake = FakeAiGateway.make(
        FakeAiGateway.sse(
          FakeAiGateway.json({
            choices: [
              {
                delta: {
                  tool_calls: [
                    {
                      index: 0,
                      id: "call_weather",
                      function: { name: "weather", arguments: '{"city":"Oslo"}' },
                    },
                  ],
                },
              },
            ],
          }),
          finish("stop"),
        ),
      );

      const events = yield* streamWithFake(fake);

      expect(events.at(-1)).toMatchObject({ type: "done", reason: "toolUse" });
    }),
  );

  it.effect("turns unknown finish reasons into terminal errors", () =>
    Effect.gen(function* () {
      const fake = FakeAiGateway.make(FakeAiGateway.sse(finish("future_reason")));

      const events = yield* streamWithFake(fake);

      expect(events).toEqual([
        expect.objectContaining({ type: "start" }),
        expect.objectContaining({
          type: "error",
          reason: "error",
          error: expect.objectContaining({
            stopReason: "error",
            errorMessage: "Provider finish_reason: future_reason",
          }),
        }),
      ]);
    }),
  );

  it.effect("emits an aborted terminal error for a pre-aborted signal", () =>
    Effect.gen(function* () {
      const controller = new AbortController();
      controller.abort();
      const fake = FakeAiGateway.make(
        FakeAiGateway.sse(
          FakeAiGateway.json({ choices: [{ delta: { content: "partial" } }] }),
          finish(),
        ),
      );

      const events = yield* streamWithFake(fake, { options: { signal: controller.signal } });

      expect(fake.gatewayCalls[0]?.options).toMatchObject({ signal: controller.signal });
      expect(events.at(-1)).toMatchObject({
        type: "error",
        reason: "aborted",
        error: { stopReason: "aborted" },
      });
      expect(JSON.stringify(events.at(-1))).toContain("aborted");
    }),
  );

  it.effect("emits a terminal error when the stream ends without DONE or finish_reason", () =>
    Effect.gen(function* () {
      const fake = FakeAiGateway.make(
        FakeAiGateway.sse(FakeAiGateway.json({ choices: [{ delta: { content: "partial" } }] })),
      );

      const events = yield* streamWithFake(fake);

      expect(events.at(-1)).toMatchObject({
        type: "error",
        reason: "error",
        error: expect.objectContaining({
          stopReason: "error",
          errorMessage: "Stream ended without [DONE] or finish_reason",
        }),
      });
    }),
  );

  it.effect("emits a terminal error for non-OK AI Gateway responses with status and body", () =>
    Effect.gen(function* () {
      const fake = FakeAiGateway.make(
        FakeAiGateway.nonOk("quota exceeded", {
          status: 429,
          statusText: "Too Many Requests",
        }),
      );

      const events = yield* streamWithFake(fake);

      expect(events).toEqual([
        expect.objectContaining({
          type: "error",
          reason: "error",
          error: expect.objectContaining({
            stopReason: "error",
            errorMessage: "Cloudflare AI Gateway returned 429 Too Many Requests: quota exceeded",
          }),
        }),
      ]);
    }),
  );

  it.effect("emits a terminal error when the model call fails", () =>
    Effect.gen(function* () {
      const fake = FakeAiGateway.make(FakeAiGateway.throws(new Error("network unavailable")));

      const events = yield* streamWithFake(fake);

      expect(events).toEqual([
        expect.objectContaining({
          type: "error",
          reason: "error",
          error: expect.objectContaining({
            stopReason: "error",
            errorMessage: "network unavailable",
          }),
        }),
      ]);
    }),
  );

  it.effect("rejects unregistered models without calling AI Gateway", () =>
    Effect.gen(function* () {
      const fake = FakeAiGateway.make(FakeAiGateway.sse(finish()));
      const invalidModel = {
        ...testModel,
        id: "unregistered-model",
      } satisfies Model<"openai-completions">;

      const events = yield* streamWithFake(fake, { model: invalidModel });

      assert.strictEqual(fake.calls.length, 0);
      assert.strictEqual(fake.gatewayCalls.length, 0);
      expect(events).toEqual([
        expect.objectContaining({
          type: "error",
          reason: "error",
          error: expect.objectContaining({
            stopReason: "error",
            errorMessage: expect.stringContaining("registry entry"),
          }),
        }),
      ]);
    }),
  );
});

const readEvent = async (
  iterator: AsyncIterator<AssistantMessageEvent>,
): Promise<AssistantMessageEvent> => {
  const result = await iterator.next();
  if (result.done) throw new Error("Expected another assistant message event.");
  return result.value;
};

const anthropicEvent = (event: string, data: unknown, delayMs?: number): FakeAiGateway.SseChunk =>
  FakeAiGateway.raw(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`, delayMs);

const openAiEvent = (data: unknown, delayMs?: number): FakeAiGateway.SseChunk =>
  FakeAiGateway.raw(`data: ${JSON.stringify(data)}\n\n`, delayMs);
