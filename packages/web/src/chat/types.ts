export type ChatStatus = "idle" | "hydrating" | "connecting" | "submitted" | "streaming" | "error";

export type ChatRole = "user" | "assistant" | "system";

export type ChatMessagePart =
  | { readonly type: "text"; readonly text: string; readonly state?: "streaming" | "done" }
  | { readonly type: "reasoning"; readonly text: string; readonly state?: "streaming" | "done" }
  | ({ readonly type: "dynamic-tool"; readonly toolName: string; readonly toolCallId: string } & (
      | {
          readonly state: "input-available";
          readonly input: unknown;
          readonly output?: never;
          readonly errorText?: never;
        }
      | {
          readonly state: "output-available";
          readonly input: unknown;
          readonly output: unknown;
          readonly errorText?: never;
        }
      | {
          readonly state: "output-error";
          readonly input: unknown;
          readonly output?: never;
          readonly errorText: string;
        }
    ))
  | { readonly type: "file"; readonly mediaType: string; readonly url: string };

export interface ChatMessage {
  readonly id: string;
  readonly role: ChatRole;
  readonly metadata?:
    | {
        readonly usage?: unknown;
        readonly model?: { readonly provider: string; readonly id: string };
        readonly [key: string]: unknown;
      }
    | undefined;
  readonly parts: ReadonlyArray<ChatMessagePart>;
}

export interface ChatSnapshot {
  readonly conversationId: string | undefined;
  readonly messages: ReadonlyArray<ChatMessage>;
  readonly status: ChatStatus;
  readonly historyReady: boolean;
  readonly error: Error | undefined;
}

interface DenoraConversationEventBase {
  readonly v: 3;
  readonly instanceId: string;
  readonly agentName: string;
  readonly eventIndex: number;
  readonly timestamp: string;
  readonly submissionId?: string | undefined;
  readonly messageId?: string | undefined;
  readonly turnId?: string | undefined;
}

export type DenoraConversationEvent =
  | (DenoraConversationEventBase & {
      readonly type: "message_start" | "message_end";
      readonly message: unknown;
    })
  | (DenoraConversationEventBase & {
      readonly type: "text_delta";
      readonly text: string;
    })
  | (DenoraConversationEventBase & {
      readonly type: "thinking_start";
      readonly contentIndex?: number | undefined;
    })
  | (DenoraConversationEventBase & {
      readonly type: "thinking_delta";
      readonly contentIndex?: number | undefined;
      readonly delta: string;
    })
  | (DenoraConversationEventBase & {
      readonly type: "thinking_end";
      readonly contentIndex?: number | undefined;
      readonly content: string;
    })
  | (DenoraConversationEventBase & {
      readonly type: "tool_start";
      readonly toolName: string;
      readonly toolCallId: string;
      readonly input?: unknown;
      readonly args?: unknown;
    })
  | (DenoraConversationEventBase & {
      readonly type: "tool";
      readonly toolName: string;
      readonly toolCallId: string;
      readonly isError: boolean;
      readonly result?: unknown;
    })
  | (DenoraConversationEventBase & {
      readonly type: "turn";
      readonly request?: unknown;
      readonly response?: unknown;
    })
  | (DenoraConversationEventBase & {
      readonly type: "submission_settled";
      readonly submissionId: string;
      readonly outcome: "completed" | "failed" | "cancelled";
      readonly result?: unknown;
      readonly error?: unknown;
    })
  | (DenoraConversationEventBase & {
      readonly type: "idle";
    })
  | (DenoraConversationEventBase & {
      readonly type: "agent_start" | "agent_end" | "turn_start" | "turn_messages";
      readonly message?: unknown;
      readonly text?: string | undefined;
      readonly outcome?: "completed" | "failed" | "cancelled" | undefined;
      readonly error?: unknown;
    });

export * as ChatTypes from "./types.ts";
