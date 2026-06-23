import { createAssistantMessageEventStream, type AssistantMessage } from "@earendil-works/pi-ai";
import type { AgentMessage, StreamFn } from "@earendil-works/pi-agent-core";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { expect } from "vitest";
import { AgentRunSession, type RunEvent } from "../../src/agent-run/AgentRunSession.ts";

const IMAGE_BYTES = "aGVsbG8taW1hZ2UtYnl0ZXM=";

describe("AgentRunSession", () => {
  it.effect("passes raw image context to Pi while emitting redacted Denora run events", () =>
    Effect.gen(function* () {
      const runId = `run_${crypto.randomUUID()}`;
      const inputMessage = {
        role: "user",
        content: [
          { type: "text", text: "describe this image" },
          { type: "image", data: IMAGE_BYTES, mimeType: "image/png" },
        ],
        timestamp: Date.now(),
      } as AgentMessage;
      const events: RunEvent[] = [];
      let providerContextMessages: unknown;

      const streamFn = ((model, context) => {
        providerContextMessages = context.messages;
        const stream = createAssistantMessageEventStream();
        const message: AssistantMessage = {
          role: "assistant",
          content: [{ type: "text", text: "described" }],
          api: model.api,
          provider: model.provider,
          model: model.id,
          usage: emptyUsage,
          stopReason: "stop",
          timestamp: Date.now(),
        };

        queueMicrotask(() => {
          stream.push({ type: "start", partial: { ...message, content: [] } });
          stream.push({ type: "text_start", contentIndex: 0, partial: message });
          stream.push({
            type: "text_delta",
            contentIndex: 0,
            delta: "described",
            partial: message,
          });
          stream.push({
            type: "text_end",
            contentIndex: 0,
            content: "described",
            partial: message,
          });
          stream.push({ type: "done", reason: "stop", message });
          stream.end();
        });

        return stream;
      }) satisfies StreamFn;

      yield* AgentRunSession.execute({
        runId,
        input: { messages: [inputMessage] },
        streamFn,
        onAgentEvent: (event) =>
          Effect.sync(() => {
            events.push(event);
          }),
      });

      expect(providerContextMessages).toContainEqual(
        expect.objectContaining({
          role: "user",
          content: expect.arrayContaining([
            expect.objectContaining({ type: "image", data: IMAGE_BYTES, mimeType: "image/png" }),
          ]),
        }),
      );
      assert.notInclude(JSON.stringify(events), IMAGE_BYTES);
      const agentEnd = events.find((event) => event.type === "agent_end");
      expect(agentEnd).toMatchObject({ type: "agent_end" });
    }),
  );
});

const emptyUsage = {
  input: 0,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  totalTokens: 0,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
};
