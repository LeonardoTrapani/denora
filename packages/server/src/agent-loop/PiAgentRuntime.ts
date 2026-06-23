import {
  Agent,
  type AgentEvent,
  type AgentMessage,
  type AgentTool,
} from "@earendil-works/pi-agent-core";
import type { Api, Model, ModelThinkingLevel } from "@earendil-works/pi-ai";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { PiRuntime } from "./PiRuntime.ts";

export class ExecutePromptFailed extends Schema.TaggedErrorClass<ExecutePromptFailed>()(
  "ExecutePromptFailed",
  {
    message: Schema.String,
    cause: Schema.Defect(),
  },
) {}

export interface ExecutePromptInput {
  readonly sessionId: string;
  readonly systemPrompt: string;
  readonly model: Model<Api>;
  readonly prompt: string | AgentMessage | ReadonlyArray<AgentMessage>;
  readonly messages?: ReadonlyArray<AgentMessage> | undefined;
  readonly tools?: ReadonlyArray<AgentTool> | undefined;
  readonly thinkingLevel?: ModelThinkingLevel | undefined;
}

export interface ExecutePromptResult {
  readonly messages: ReadonlyArray<AgentMessage>;
  readonly newMessages: ReadonlyArray<AgentMessage>;
  readonly assistantText: string;
  readonly events: ReadonlyArray<AgentEvent>;
}

/**
 * Non-durable prompt runner used as a service API around pi-agent-core. Unlike
 * AgentRunSession, this captures raw Pi AgentEvents for callers instead of
 * translating them into Denora's durable public run-event contract.
 */
export interface Interface {
  readonly executePrompt: (
    input: ExecutePromptInput,
  ) => Effect.Effect<ExecutePromptResult, ExecutePromptFailed>;
}

export class Service extends Context.Service<Service, Interface>()(
  "@denora/server/PiAgentRuntime",
) {}

export const layer: Layer.Layer<Service, never, PiRuntime.Service> = Layer.effect(
  Service,
  Effect.gen(function* () {
    const pi = yield* PiRuntime.Service;

    const executePrompt = Effect.fn("PiAgentRuntime.executePrompt")(function* (
      input: ExecutePromptInput,
    ) {
      return yield* Effect.tryPromise({
        try: async () => {
          const events: Array<AgentEvent> = [];
          const agent = new Agent({
            initialState: {
              systemPrompt: input.systemPrompt,
              model: input.model,
              tools: [...(input.tools ?? [])],
              messages: [...(input.messages ?? [])],
              thinkingLevel: input.thinkingLevel ?? "medium",
            },
            streamFn: pi.streamFn,
            toolExecution: "parallel",
            sessionId: input.sessionId,
          });

          const checkpoint = agent.state.messages.length;
          agent.subscribe((event) => {
            // TODO: mirror Flue's event callback/journal boundary when the
            // Conversation Durable Object starts coordinating execution.
            events.push(event);
          });

          if (typeof input.prompt === "string") {
            await agent.prompt(input.prompt);
          } else if (isAgentMessageArray(input.prompt)) {
            await agent.prompt([...input.prompt]);
          } else {
            await agent.prompt(input.prompt);
          }

          const messages = agent.state.messages.slice();
          const newMessages = messages.slice(checkpoint);

          return {
            messages,
            newMessages,
            assistantText: assistantTextFrom(newMessages),
            events,
          };
        },
        catch: (cause) =>
          new ExecutePromptFailed({
            message: cause instanceof Error ? cause.message : "Pi agent prompt execution failed.",
            cause,
          }),
      });
    });

    return Service.of({ executePrompt });
  }),
);

const assistantTextFrom = (messages: ReadonlyArray<AgentMessage>): string => {
  const latest = findLatestAssistant(messages);
  if (latest === undefined) return "";
  return latest.content.flatMap((block) => (block.type === "text" ? [block.text] : [])).join("");
};

const isAgentMessageArray = (
  value: AgentMessage | ReadonlyArray<AgentMessage>,
): value is ReadonlyArray<AgentMessage> => Array.isArray(value);

const findLatestAssistant = (
  messages: ReadonlyArray<AgentMessage>,
): Extract<AgentMessage, { role: "assistant" }> | undefined => {
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const message = messages[index];
    if (message?.role === "assistant") return message;
  }
  return undefined;
};

export * as PiAgentRuntime from "./PiAgentRuntime.ts";
