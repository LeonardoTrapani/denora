import {
  Agent,
  type AgentEvent,
  type AgentMessage,
  type StreamFn,
} from "@earendil-works/pi-agent-core";
import type { Api, Model } from "@earendil-works/pi-ai";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { toTurnMessage } from "../agent-loop/PiAgentModel.ts";

export interface RunEvent {
  readonly type: string;
  readonly [key: string]: unknown;
}

export type RunEventCallback = (event: RunEvent) => Effect.Effect<void, unknown>;

export interface ExecuteInput {
  readonly runId: string;
  readonly input?: unknown;
  readonly streamFn: StreamFn;
  readonly onAgentEvent: RunEventCallback;
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

class AgentRunSession {
  private readonly agentLoop: Agent;
  private readonly eventCallback: RunEventCallback;
  private readonly input: ExecuteInput;
  private activeTurnId: string | undefined;
  private readonly activeToolCalls = new Map<
    string,
    { readonly startedAt: number; readonly toolName: string }
  >();

  constructor(input: ExecuteInput) {
    this.input = input;
    this.eventCallback = input.onAgentEvent;
    this.agentLoop = new Agent({
      initialState: {
        systemPrompt: systemPromptFrom(input.input),
        model: modelFrom(input.input),
        tools: [],
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
        const prompt = promptFrom(this.input.input);
        if (prompt.length > 0) await this.agentLoop.prompt(prompt);
        else await this.agentLoop.continue();

        const messages = this.agentLoop.state.messages.slice();
        return {
          messages,
          assistantText: assistantTextFrom(messages),
        };
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
        await this.emit({ type: "turn_start", turnId: this.activeTurnId, purpose: "agent" });
        break;
      case "message_start": {
        const turnId = this.activeTurnId ?? generateTurnId();
        this.activeTurnId = turnId;
        await this.emit({ type: "message_start", message: event.message, turnId });
        break;
      }
      case "message_update": {
        const aEvent = event.assistantMessageEvent;
        if (aEvent.type === "text_delta") {
          await this.emit({ type: "text_delta", text: aEvent.delta });
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
          await this.emit({
            type: "turn",
            turnId,
            purpose: "agent",
            response: { output: toTurnMessage(event.message) },
          });
        }
        await this.emit({ type: "message_end", message: event.message, turnId });
        break;
      }
      case "tool_execution_start":
        this.activeToolCalls.set(event.toolCallId, {
          startedAt: Date.now(),
          toolName: event.toolName,
        });
        await this.emit({
          type: "tool_start",
          toolName: event.toolName,
          toolCallId: event.toolCallId,
        });
        break;
      case "tool_execution_update":
        break;
      case "tool_execution_end": {
        const call = this.activeToolCalls.get(event.toolCallId);
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
        break;
      }
      case "agent_end":
        await this.emit({ type: "agent_end", messages: event.messages });
        this.activeTurnId = undefined;
        break;
    }
  }

  private emit(event: RunEvent): Promise<void> {
    const decorated = {
      ...event,
      runId: this.input.runId,
      ...(this.activeTurnId !== undefined && event.turnId === undefined
        ? { turnId: this.activeTurnId }
        : {}),
    };
    return Effect.runPromise(this.eventCallback(decorated));
  }
}

const DEFAULT_MODEL_ID = "@cf/meta/llama-3.1-8b-instruct";

const defaultModel = {
  id: DEFAULT_MODEL_ID,
  name: DEFAULT_MODEL_ID,
  api: "openai-completions",
  provider: "cloudflare-workers-ai",
  baseUrl: "",
  reasoning: true,
  input: ["text", "image"],
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
  contextWindow: 128_000,
  maxTokens: 8192,
} satisfies Model<"openai-completions">;

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

export * as AgentRunSession from "./AgentRunSession.ts";
