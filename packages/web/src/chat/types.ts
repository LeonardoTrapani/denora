import type { AttachedAgentEvent } from "@denora/server/stream-events";

export type ChatStatus = "idle" | "connecting" | "submitted" | "streaming" | "error";

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

export type DenoraConversationEvent = AttachedAgentEvent;

export * as ChatTypes from "./types.ts";
