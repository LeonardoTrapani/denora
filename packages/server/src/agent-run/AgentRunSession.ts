import {
  Agent,
  type AgentEvent,
  type AgentMessage,
  type AgentTool,
  type StreamFn,
} from "@earendil-works/pi-agent-core";
import type { Api, AssistantMessage, AssistantMessageEvent, Model } from "@earendil-works/pi-ai";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { CloudflareAiGatewayModels } from "../agent-loop/CloudflareAiGatewayModels.ts";
import { redactRunEventImages, type RunEvent, toTurnMessage } from "./RunEventContract.ts";

export type { RunEvent } from "./RunEventContract.ts";

export type RunEventCallback = (event: RunEvent) => Effect.Effect<void, unknown>;
export type AssistantStreamEventCallback = (
  event: AssistantMessageEvent,
) => Effect.Effect<void, unknown>;

export type RunCheckpoint =
  | {
      readonly type: "assistant_message_started";
      readonly runId: string;
      readonly messageIndex: number;
    }
  | {
      readonly type: "assistant_text_part_completed";
      readonly runId: string;
      readonly messageIndex: number;
      readonly contentIndex: number;
      readonly text: string;
    }
  | {
      readonly type: "assistant_message_completed";
      readonly runId: string;
      readonly messageIndex: number;
      readonly message: Extract<AgentMessage, { role: "assistant" }>;
    }
  | {
      readonly type: "tool_call_started";
      readonly checkpointId: string;
      readonly runId: string;
      readonly toolCallId: string;
      readonly toolName: string;
      readonly args: unknown;
    }
  | {
      readonly type: "tool_result_completed";
      readonly checkpointId: string;
      readonly runId: string;
      readonly toolCallId: string;
      readonly toolName: string;
      readonly result: unknown;
      readonly isError: boolean;
    };

export type RunCheckpointCallback = (checkpoint: RunCheckpoint) => Effect.Effect<void, unknown>;

export interface ExecuteInput {
  readonly runId: string;
  readonly input?: unknown;
  readonly streamFn: StreamFn;
  readonly tools?: ReadonlyArray<AgentTool<any>> | undefined;
  readonly onAgentEvent: RunEventCallback;
  readonly onAssistantStreamEvent?: AssistantStreamEventCallback | undefined;
  readonly onCheckpoint?: RunCheckpointCallback | undefined;
  readonly initialAssistantMessageIndex?: number | undefined;
  readonly signal?: AbortSignal | undefined;
}

export interface ExecuteResult {
  readonly messages: ReadonlyArray<AgentMessage>;
  readonly assistantText: string;
}

export class AgentRunSessionFailed extends Schema.TaggedErrorClass<AgentRunSessionFailed>()(
  "AgentRunSessionFailed",
  {
    message: Schema.String,
    cause: Schema.Defect(),
  },
) {}

export const execute = Effect.fn("AgentRunSession.execute")(function* (
  input: ExecuteInput,
): Effect.fn.Return<ExecuteResult, AgentRunSessionFailed> {
  const session = new AgentRunSession(input);
  return yield* session.prompt();
});

// Pi-to-Denora event boundary: this class owns translating pi-agent-core
// AgentEvents into Denora's public run stream events. Provider-stream parsing
// is lower in PiAgentModel; durable persistence is higher in Lifecycle.
class AgentRunSession {
  private readonly agentLoop: Agent;
  private readonly eventCallback: RunEventCallback;
  private readonly assistantStreamEventCallback: AssistantStreamEventCallback | undefined;
  private readonly checkpointCallback: RunCheckpointCallback | undefined;
  private readonly input: ExecuteInput;
  private activeTurnId: string | undefined;
  private activeTurnStartedAt: number | undefined;
  private activeAssistantMessageIndex: number | undefined;
  private nextAssistantMessageIndex: number;
  private readonly activeToolCalls = new Map<
    string,
    { readonly startedAt: number; readonly toolName: string }
  >();

  constructor(input: ExecuteInput) {
    this.input = input;
    this.eventCallback = input.onAgentEvent;
    this.assistantStreamEventCallback = input.onAssistantStreamEvent;
    this.checkpointCallback = input.onCheckpoint;
    this.nextAssistantMessageIndex = input.initialAssistantMessageIndex ?? 0;
    this.agentLoop = new Agent({
      initialState: {
        systemPrompt: systemPromptFrom(input.input),
        model: modelFrom(input.input),
        tools: toolsFrom(input),
        messages: messagesFrom(input.input),
        thinkingLevel: thinkingLevelFrom(input.input),
      },
      streamFn: input.streamFn,
      toolExecution: "parallel",
      sessionId: input.runId,
    });

    this.agentLoop.subscribe((event) => this.handleAgentEvent(event));
  }

  prompt(): Effect.Effect<ExecuteResult, AgentRunSessionFailed> {
    return Effect.tryPromise({
      try: async () => {
        const onAbort = () => this.agentLoop.abort();
        if (this.input.signal?.aborted) onAbort();
        else this.input.signal?.addEventListener("abort", onAbort, { once: true });
        const prompt = promptFrom(this.input.input);
        try {
          if (prompt.length > 0) await this.agentLoop.prompt(prompt);
          else await this.agentLoop.continue();
          if (this.input.signal?.aborted) throw new Error("Agent run aborted.");

          const messages = this.agentLoop.state.messages.slice();
          return {
            messages,
            assistantText: assistantTextFrom(messages),
          };
        } finally {
          this.input.signal?.removeEventListener("abort", onAbort);
        }
      },
      catch: (cause) =>
        new AgentRunSessionFailed({
          message: cause instanceof Error ? cause.message : "Agent run session failed.",
          cause,
        }),
    });
  }

  private async handleAgentEvent(event: AgentEvent): Promise<void> {
    switch (event.type) {
      case "agent_start":
        await this.emit({ type: "agent_start" });
        break;
      case "turn_start":
        this.activeTurnId ??= generateTurnId();
        this.activeTurnStartedAt = Date.now();
        await this.emit({ type: "turn_start", turnId: this.activeTurnId, purpose: "agent" });
        break;
      case "message_start": {
        const turnId = this.activeTurnId ?? generateTurnId();
        this.activeTurnId = turnId;
        if (event.message.role === "assistant") {
          const messageIndex = this.assistantMessageIndex();
          await this.checkpoint({
            type: "assistant_message_started",
            runId: this.input.runId,
            messageIndex,
          });
        }
        await this.emit({ type: "message_start", message: event.message, turnId });
        break;
      }
      case "message_update": {
        const aEvent = event.assistantMessageEvent;
        await this.assistantStreamEvent(aEvent);
        if (aEvent.type === "text_delta") {
          await this.emit({ type: "text_delta", text: aEvent.delta });
        } else if (aEvent.type === "text_end") {
          await this.checkpoint({
            type: "assistant_text_part_completed",
            runId: this.input.runId,
            messageIndex: this.assistantMessageIndex(),
            contentIndex: aEvent.contentIndex,
            text: aEvent.content,
          });
        } else if (aEvent.type === "thinking_start") {
          await this.emit({ type: "thinking_start", contentIndex: aEvent.contentIndex });
        } else if (aEvent.type === "thinking_delta") {
          await this.emit({
            type: "thinking_delta",
            contentIndex: aEvent.contentIndex,
            delta: aEvent.delta,
          });
        } else if (aEvent.type === "thinking_end") {
          await this.emit({
            type: "thinking_end",
            contentIndex: aEvent.contentIndex,
            content: aEvent.content,
          });
        }
        break;
      }
      case "message_end": {
        const turnId = this.activeTurnId ?? generateTurnId();
        this.activeTurnId = turnId;
        if (event.message.role === "assistant") {
          const messageIndex = this.assistantMessageIndex();
          await this.emit({
            type: "turn",
            turnId,
            purpose: "agent",
            durationMs: this.turnDurationMs(),
            request: requestInfoFrom(event.message),
            response: responseInfoFrom(event.message),
            isError:
              event.message.stopReason === "error" || event.message.errorMessage !== undefined,
          });
          await this.checkpoint({
            type: "assistant_message_completed",
            runId: this.input.runId,
            messageIndex,
            message: event.message,
          });
          this.nextAssistantMessageIndex = Math.max(
            this.nextAssistantMessageIndex,
            messageIndex + 1,
          );
          this.activeAssistantMessageIndex = undefined;
        }
        await this.emit({ type: "message_end", message: event.message, turnId });
        break;
      }
      case "tool_execution_start":
        this.activeToolCalls.set(event.toolCallId, {
          startedAt: Date.now(),
          toolName: event.toolName,
        });
        await this.checkpoint({
          type: "tool_call_started",
          checkpointId: toolCallCheckpointId(this.input.runId, event.toolCallId),
          runId: this.input.runId,
          toolCallId: event.toolCallId,
          toolName: event.toolName,
          args: event.args,
        });
        await this.emit({
          type: "tool_start",
          toolName: event.toolName,
          toolCallId: event.toolCallId,
          args: event.args,
        });
        break;
      case "tool_execution_update":
        break;
      case "tool_execution_end": {
        const call = this.activeToolCalls.get(event.toolCallId);
        await this.checkpoint({
          type: "tool_result_completed",
          checkpointId: toolResultCheckpointId(this.input.runId, event.toolCallId),
          runId: this.input.runId,
          toolCallId: event.toolCallId,
          toolName: event.toolName,
          result: event.result,
          isError: event.isError,
        });
        await this.emit({
          type: "tool",
          toolName: event.toolName,
          toolCallId: event.toolCallId,
          isError: event.isError,
          result: event.result,
          durationMs: call === undefined ? 0 : Math.max(0, Date.now() - call.startedAt),
        });
        this.activeToolCalls.delete(event.toolCallId);
        break;
      }
      case "turn_end": {
        const turnId = this.activeTurnId ?? generateTurnId();
        await this.emit({
          type: "turn_messages",
          turnId,
          purpose: "agent",
          message: event.message,
          toolResults: event.toolResults,
        });
        this.activeTurnId = undefined;
        this.activeTurnStartedAt = undefined;
        break;
      }
      case "agent_end":
        await this.emit({ type: "agent_end", messages: event.messages });
        this.activeTurnId = undefined;
        this.activeTurnStartedAt = undefined;
        break;
    }
  }

  private emit(event: RunEvent): Promise<void> {
    const decorated = {
      ...redactRunEventImages(event),
      runId: this.input.runId,
      ...(this.activeTurnId !== undefined && event.turnId === undefined
        ? { turnId: this.activeTurnId }
        : {}),
    };
    return Effect.runPromise(this.eventCallback(decorated));
  }

  private checkpoint(checkpoint: RunCheckpoint): Promise<void> {
    return this.checkpointCallback === undefined
      ? Promise.resolve()
      : Effect.runPromise(this.checkpointCallback(checkpoint));
  }

  private assistantStreamEvent(event: AssistantMessageEvent): Promise<void> {
    return this.assistantStreamEventCallback === undefined
      ? Promise.resolve()
      : Effect.runPromise(this.assistantStreamEventCallback(event));
  }

  private assistantMessageIndex(): number {
    if (this.activeAssistantMessageIndex === undefined)
      this.activeAssistantMessageIndex = this.nextAssistantMessageIndex;
    return this.activeAssistantMessageIndex;
  }

  private turnDurationMs(): number {
    return this.activeTurnStartedAt === undefined
      ? 0
      : Math.max(0, Date.now() - this.activeTurnStartedAt);
  }
}

const requestInfoFrom = (message: AssistantMessage): Record<string, unknown> => ({
  providerId: message.provider,
  providerName: message.provider,
  requestedModel: message.model,
  api: message.api,
});

const responseInfoFrom = (message: AssistantMessage): Record<string, unknown> => ({
  ...(message.responseId === undefined ? {} : { responseId: message.responseId }),
  ...(message.responseModel === undefined ? {} : { responseModel: message.responseModel }),
  output: toTurnMessage(message),
  usage: message.usage,
  finishReason: message.stopReason,
  ...(message.errorMessage === undefined ? {} : { error: { message: message.errorMessage } }),
});

const defaultModel = CloudflareAiGatewayModels.defaultModel;

const systemPromptFrom = (input: unknown): string =>
  stringField(input, "systemPrompt") ?? "You are Denora, a secure personal agent.";

const modelFrom = (input: unknown): Model<Api> => {
  const model = recordField(input, "model");
  if (model === undefined) return defaultModel;
  return model as unknown as Model<Api>;
};

const messagesFrom = (input: unknown): AgentMessage[] => {
  const messages = arrayField(input, "messages");
  return messages === undefined ? [] : (messages as AgentMessage[]).slice();
};

const toolsFrom = (input: ExecuteInput): AgentTool<any>[] => {
  if (input.tools !== undefined) return input.tools.slice();
  const tools = arrayField(input.input, "tools");
  return tools === undefined ? [] : (tools as AgentTool<any>[]).slice();
};

const thinkingLevelFrom = (
  input: unknown,
): "off" | "minimal" | "low" | "medium" | "high" | "xhigh" => {
  const value = stringField(input, "thinkingLevel");
  return value === "off" ||
    value === "minimal" ||
    value === "low" ||
    value === "medium" ||
    value === "high" ||
    value === "xhigh"
    ? value
    : "medium";
};

const promptFrom = (input: unknown): string => {
  if (typeof input === "string") return input;
  return stringField(input, "prompt") ?? "";
};

const stringField = (input: unknown, field: string): string | undefined => {
  const value = recordField(input, field);
  return typeof value === "string" ? value : undefined;
};

const arrayField = (input: unknown, field: string): ReadonlyArray<unknown> | undefined => {
  const value = recordField(input, field);
  return Array.isArray(value) ? value : undefined;
};

const recordField = (input: unknown, field: string): unknown =>
  typeof input === "object" && input !== null
    ? (input as Record<string, unknown>)[field]
    : undefined;

const assistantTextFrom = (messages: ReadonlyArray<AgentMessage>): string => {
  const latest = findLatestAssistant(messages);
  if (latest === undefined) return "";
  return latest.content.flatMap((block) => (block.type === "text" ? [block.text] : [])).join("");
};

const findLatestAssistant = (
  messages: ReadonlyArray<AgentMessage>,
): Extract<AgentMessage, { role: "assistant" }> | undefined => {
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const message = messages[index];
    if (message?.role === "assistant") return message;
  }
  return undefined;
};

const generateTurnId = (): string => `turn_${crypto.randomUUID()}`;

const toolCallCheckpointId = (runId: string, toolCallId: string): string =>
  `tool-call:${runId}:${toolCallId}`;

const toolResultCheckpointId = (runId: string, toolCallId: string): string =>
  `tool-result:${runId}:${toolCallId}`;

export * as AgentRunSession from "./AgentRunSession.ts";
