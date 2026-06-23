import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
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
