import {
  createAssistantMessageEventStream,
  Type,
  type AssistantMessage,
} from "@earendil-works/pi-ai";
import type { AgentMessage, AgentTool, StreamFn } from "@earendil-works/pi-agent-core";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { expect } from "vitest";
import {
  AgentRunSession,
  type RunCheckpoint,
  type RunEvent,
} from "../../src/agent-run/AgentRunSession.ts";

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

  it.effect("passes Pi tools into the agent loop and checkpoints tool boundaries", () =>
    Effect.gen(function* () {
      const runId = `run_${crypto.randomUUID()}`;
      const events: RunEvent[] = [];
      const checkpoints: RunCheckpoint[] = [];
      const contexts: Array<{ readonly tools?: unknown; readonly roles: ReadonlyArray<string> }> =
        [];
      let executedArgs: unknown;
      const sampleParams = Type.Object({ value: Type.String() });
      const tool: AgentTool<any> = {
        name: "sample_tool",
        label: "Sample Tool",
        description: "Returns a sample result.",
        parameters: sampleParams,
        async execute(_toolCallId, params) {
          const input = params as { readonly value: string };
          executedArgs = input;
          return {
            content: [{ type: "text", text: `tool:${input.value}` }],
            details: { value: input.value },
          };
        },
      };

      const streamFn = ((model, context) => {
        contexts.push({
          tools: context.tools,
          roles: context.messages.map((message) => message.role),
        });
        const stream = createAssistantMessageEventStream();
        const hasToolResult = context.messages.some((message) => message.role === "toolResult");
        const message: AssistantMessage = hasToolResult
          ? {
              role: "assistant",
              content: [{ type: "text", text: "final" }],
              api: model.api,
              provider: model.provider,
              model: model.id,
              usage: emptyUsage,
              stopReason: "stop",
              timestamp: Date.now(),
            }
          : {
              role: "assistant",
              content: [
                {
                  type: "toolCall",
                  id: "call_1",
                  name: "sample_tool",
                  arguments: { value: "ok" },
                },
              ],
              api: model.api,
              provider: model.provider,
              model: model.id,
              usage: emptyUsage,
              stopReason: "toolUse",
              timestamp: Date.now(),
            };

        queueMicrotask(() => {
          stream.push({ type: "start", partial: { ...message, content: [] } });
          if (message.content[0]?.type === "text") {
            stream.push({ type: "text_start", contentIndex: 0, partial: message });
            stream.push({ type: "text_delta", contentIndex: 0, delta: "final", partial: message });
            stream.push({ type: "text_end", contentIndex: 0, content: "final", partial: message });
          } else {
            const toolCall = message.content[0];
            if (toolCall?.type !== "toolCall") throw new Error("Expected tool call content.");
            stream.push({ type: "toolcall_start", contentIndex: 0, partial: message });
            stream.push({
              type: "toolcall_end",
              contentIndex: 0,
              toolCall,
              partial: message,
            });
          }
          stream.push({
            type: "done",
            reason: message.stopReason === "toolUse" ? "toolUse" : "stop",
            message,
          });
          stream.end();
        });

        return stream;
      }) satisfies StreamFn;

      const result = yield* AgentRunSession.execute({
        runId,
        input: { prompt: "use the tool" },
        streamFn,
        tools: [tool],
        onAgentEvent: (event) =>
          Effect.sync(() => {
            events.push(event);
          }),
        onCheckpoint: (checkpoint) =>
          Effect.sync(() => {
            checkpoints.push(checkpoint);
          }),
      });

      assert.deepStrictEqual(executedArgs, { value: "ok" });
      assert.strictEqual(result.assistantText, "final");
      expect(contexts[0]?.tools).toContain(tool);
      assert.deepStrictEqual(contexts[1]?.roles, ["user", "assistant", "toolResult"]);
      expect(checkpoints).toContainEqual(
        expect.objectContaining({
          type: "tool_call_started",
          checkpointId: `tool-call:${runId}:call_1`,
          toolCallId: "call_1",
          toolName: "sample_tool",
          args: { value: "ok" },
        }),
      );
      expect(checkpoints).toContainEqual(
        expect.objectContaining({
          type: "tool_result_completed",
          checkpointId: `tool-result:${runId}:call_1`,
          toolCallId: "call_1",
          toolName: "sample_tool",
          isError: false,
        }),
      );
      expect(events.map((event) => event.type)).toEqual(
        expect.arrayContaining(["tool_start", "tool", "agent_end"]),
      );
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
