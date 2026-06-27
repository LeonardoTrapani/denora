import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { RunEventContract } from "../../src/agent-run/RunEventContract.ts";

const IMAGE_BYTES = "aGVsbG8taW1hZ2UtYnl0ZXM=";

describe("RunEventContract", () => {
  it.effect("redacts image bytes from message-bearing public run events", () =>
    Effect.sync(() => {
      const userMessage = {
        role: "user" as const,
        content: [
          { type: "text" as const, text: "look" },
          { type: "image" as const, data: IMAGE_BYTES, mimeType: "image/png" },
        ],
        timestamp: Date.now(),
      };
      const toolResult = {
        role: "toolResult" as const,
        toolCallId: "call_screenshot",
        toolName: "screenshot",
        content: [{ type: "image" as const, data: IMAGE_BYTES, mimeType: "image/png" }],
        isError: false,
        timestamp: Date.now(),
      };

      const events = [
        RunEventContract.redactRunEventImages({ type: "message_start", message: userMessage }),
        RunEventContract.redactRunEventImages({ type: "message_end", message: userMessage }),
        RunEventContract.redactRunEventImages({
          type: "turn_messages",
          message: userMessage,
          toolResults: [toolResult],
        }),
        RunEventContract.redactRunEventImages({ type: "agent_end", messages: [userMessage] }),
        RunEventContract.redactRunEventImages({
          type: "tool",
          result: { content: [{ type: "image", data: IMAGE_BYTES, mimeType: "image/png" }] },
        }),
      ];

      assert.notInclude(JSON.stringify(events), IMAGE_BYTES);
      assert.include(JSON.stringify(events), RunEventContract.IMAGE_DATA_OMITTED);
      assert.strictEqual(userMessage.content[1]?.data, IMAGE_BYTES);
      assert.strictEqual(toolResult.content[0]?.data, IMAGE_BYTES);
    }),
  );

  it.effect("projects image content to public turn payloads without raw bytes", () =>
    Effect.sync(() => {
      const content = RunEventContract.toTurnContent({
        type: "image",
        data: IMAGE_BYTES,
        mimeType: "image/png",
      });

      assert.deepStrictEqual(content, {
        type: "image",
        data: RunEventContract.IMAGE_DATA_OMITTED,
        mimeType: "image/png",
      });
      assert.notInclude(JSON.stringify(content), IMAGE_BYTES);
    }),
  );

  it.effect("accepts Flue-shaped attached agent events", () =>
    Effect.sync(() => {
      const decode = Schema.decodeUnknownSync(RunEventContract.PublicConversationEvent);
      const decoded = decode({
        v: 3,
        type: "message_start",
        instanceId: "conversation-1",
        conversationId: "conversation-1",
        agentName: "denora",
        dispatchId: "dispatch-1",
        submissionId: "submission-1",
        turnId: "turn-1",
        eventIndex: 0,
        timestamp: "2026-06-12T00:00:00.000Z",
        message: {
          role: "assistant",
          content: [
            { type: "thinking", thinking: "checking" },
            { type: "text", text: "hello" },
            { type: "toolCall", id: "tool-1", name: "search", arguments: { q: "denora" } },
          ],
        },
      });

      assert.strictEqual(decoded.type, "message_start");
      assert.strictEqual(decoded.turnId, "turn-1");
      assert.strictEqual(decoded.dispatchId, "dispatch-1");
      assert.strictEqual(decoded.conversationId, "conversation-1");
      assert.notProperty(decoded, "messageId");
      assert.notProperty(decoded, "runId");
    }),
  );

  it.effect("rejects message events without turnId and public messageId", () =>
    Effect.sync(() => {
      const decode = Schema.decodeUnknownSync(RunEventContract.PublicConversationEvent);

      assert.throws(() =>
        decode({
          v: 3,
          type: "message_start",
          instanceId: "conversation-1",
          agentName: "denora",
          submissionId: "submission-1",
          eventIndex: 0,
          timestamp: "2026-06-12T00:00:00.000Z",
          message: { role: "user", content: "hello" },
        }),
      );
      assert.throws(() =>
        decode({
          v: 3,
          type: "message_start",
          instanceId: "conversation-1",
          agentName: "denora",
          submissionId: "submission-1",
          turnId: "turn-1",
          messageId: "message-1",
          eventIndex: 0,
          timestamp: "2026-06-12T00:00:00.000Z",
          message: { role: "user", content: "hello" },
        }),
      );
    }),
  );

  it.effect("aligns submission_settled outcomes with Flue", () =>
    Effect.sync(() => {
      const decode = Schema.decodeUnknownSync(RunEventContract.PublicConversationEvent);
      assert.strictEqual(
        decode({
          v: 3,
          type: "submission_settled",
          instanceId: "conversation-1",
          submissionId: "submission-1",
          eventIndex: 2,
          timestamp: "2026-06-12T00:00:00.000Z",
          outcome: "failed",
          error: { message: "cancelled by user", type: "cancelled" },
        }).outcome,
        "failed",
      );
      assert.throws(() =>
        decode({
          v: 3,
          type: "submission_settled",
          instanceId: "conversation-1",
          submissionId: "submission-1",
          eventIndex: 2,
          timestamp: "2026-06-12T00:00:00.000Z",
          outcome: "cancelled",
        }),
      );
    }),
  );

  it.effect("classifies durable run stream exclusions and buffered event types", () =>
    Effect.sync(() => {
      assert.isTrue(RunEventContract.isStreamExcludedRunEvent({ type: "turn_request" }));
      assert.isFalse(RunEventContract.isStreamExcludedRunEvent({ type: "turn_messages" }));
      assert.isTrue(RunEventContract.isBufferedRunEvent({ type: "text_delta", text: "hi" }));
      assert.isTrue(RunEventContract.isBufferedRunEvent({ type: "thinking_delta", delta: "hmm" }));
      assert.isFalse(RunEventContract.isBufferedRunEvent({ type: "message_end" }));
    }),
  );
});
