import {
  Attachment,
  AttachmentContent,
  AttachmentDescription,
  AttachmentMedia,
  AttachmentTitle,
} from "@denora/ui/components/attachment";
import { Bubble, BubbleContent } from "@denora/ui/components/bubble";
import { Marker, MarkerContent } from "@denora/ui/components/marker";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "@denora/ui/components/empty";
import {
  InputGroup,
  InputGroupAddon,
  InputGroupButton,
  InputGroupTextarea,
} from "@denora/ui/components/input-group";
import {
  MessageScroller,
  MessageScrollerButton,
  MessageScrollerContent,
  MessageScrollerItem,
  MessageScrollerProvider,
  MessageScrollerViewport,
} from "@denora/ui/components/message-scroller";
import { Reasoning, ReasoningContent, ReasoningTrigger } from "@denora/ui/components/reasoning";
import { Response } from "@denora/ui/components/response";
import { SidebarTrigger } from "@denora/ui/components/sidebar";
import { Spinner } from "@denora/ui/components/spinner";
import {
  Tool,
  ToolContent,
  ToolHeader,
  ToolSection,
  type ToolStatus,
} from "@denora/ui/components/tool";
import { IconAlertTriangle, IconArrowUp, IconSparkles } from "@tabler/icons-react";
import { useAtomSet } from "@effect/atom-react";
import { useState, type FormEvent } from "react";

import { loadConversationsAtom } from "./atoms.ts";
import { LoadingStates } from "./LoadingStates.tsx";
import type { ChatMessage, ChatMessagePart, ChatStatus } from "./types.ts";
import type { UseConversationChatResult } from "./useConversationChat.ts";

export interface Props {
  readonly chat: UseConversationChatResult;
  readonly title?: string | null | undefined;
}

export function View({ chat, title }: Props) {
  const [composerText, setComposerText] = useState("");
  const [sendPending, setSendPending] = useState(false);
  const [sendError, setSendError] = useState<Error | undefined>(undefined);
  const refreshConversations = useAtomSet(loadConversationsAtom, { mode: "promise" });
  const canSend = composerText.trim().length > 0 && !sendPending;
  const showHistorySkeleton = !chat.historyReady && chat.messages.length === 0;
  const error = sendError ?? chat.error;

  const send = (event: FormEvent) => {
    event.preventDefault();
    const message = composerText.trim();
    if (message.length === 0 || sendPending) return;
    setComposerText("");
    setSendPending(true);
    setSendError(undefined);
    void chat
      .sendMessage(message)
      .then(() => {
        void refreshConversations();
      })
      .catch((cause: unknown) => {
        setComposerText(message);
        setSendError(toError(cause));
      })
      .finally(() => setSendPending(false));
  };

  return (
    <div className="flex h-svh min-h-0 flex-col overflow-hidden">
      <header className="flex h-14 shrink-0 items-center gap-2 border-b px-4">
        <SidebarTrigger />
        <h1 className="truncate text-sm font-medium">{title ?? "New chat"}</h1>
        <ChatStatus status={chat.status} historyReady={chat.historyReady} />
      </header>

      <section className="flex min-h-0 flex-1 flex-col overflow-hidden">
        <MessageScrollerProvider
          autoScroll
          defaultScrollPosition="last-anchor"
          scrollPreviousItemPeek={64}
        >
          <MessageScroller className="flex-1">
            <MessageScrollerViewport>
              <MessageScrollerContent
                aria-busy={chat.status === "streaming" || !chat.historyReady}
                className="mx-auto w-full max-w-3xl gap-8 px-4 py-6"
              >
                {showHistorySkeleton ? (
                  <MessageScrollerItem messageId="loading-history">
                    <LoadingStates.ConversationHistorySkeleton />
                  </MessageScrollerItem>
                ) : chat.messages.length === 0 ? (
                  <MessageScrollerItem messageId="empty">
                    <EmptyChat />
                  </MessageScrollerItem>
                ) : (
                  chat.messages.map((message) => (
                    <MessageScrollerItem
                      key={message.id}
                      messageId={message.id}
                      scrollAnchor={message.role === "user"}
                    >
                      <ChatMessageView message={message} />
                    </MessageScrollerItem>
                  ))
                )}
              </MessageScrollerContent>
            </MessageScrollerViewport>
            <MessageScrollerButton />
          </MessageScroller>
        </MessageScrollerProvider>

        <div className="shrink-0 px-4 pb-4">
          {error ? (
            <div className="mx-auto mb-2 flex w-full max-w-3xl items-center gap-2 text-sm text-destructive">
              <IconAlertTriangle className="size-4 shrink-0" />
              <span>{error.message}</span>
            </div>
          ) : null}
          <form className="mx-auto w-full max-w-3xl" onSubmit={send}>
            <InputGroup>
              <InputGroupTextarea
                disabled={sendPending}
                placeholder="Message Denora…"
                rows={1}
                value={composerText}
                onChange={(event) => setComposerText(event.currentTarget.value)}
                onKeyDown={(event) => {
                  if (event.key === "Enter" && !event.shiftKey) {
                    event.preventDefault();
                    event.currentTarget.form?.requestSubmit();
                  }
                }}
              />
              <InputGroupAddon align="block-end">
                <InputGroupButton
                  aria-label="Send message"
                  className="ml-auto rounded-full"
                  disabled={!canSend}
                  size="icon-sm"
                  type="submit"
                  variant="default"
                >
                  {sendPending ? <Spinner /> : <IconArrowUp />}
                </InputGroupButton>
              </InputGroupAddon>
            </InputGroup>
          </form>
        </div>
      </section>
    </div>
  );
}

function ChatStatus({
  status,
  historyReady,
}: {
  readonly status: ChatStatus;
  readonly historyReady: boolean;
}) {
  const busy = status === "streaming" || status === "submitted" || status === "connecting";
  if (historyReady && !busy) return null;
  return (
    <span className="ml-auto flex items-center gap-1.5 text-xs text-muted-foreground">
      <Spinner className="size-3.5" />
      {historyReady ? statusLabel(status) : "Syncing history…"}
    </span>
  );
}

function EmptyChat() {
  return (
    <Empty className="py-24">
      <EmptyHeader>
        <EmptyMedia variant="icon">
          <IconSparkles />
        </EmptyMedia>
        <EmptyTitle>Start a conversation</EmptyTitle>
        <EmptyDescription>Ask your Denora agent anything to get going.</EmptyDescription>
      </EmptyHeader>
    </Empty>
  );
}

function ChatMessageView({ message }: { readonly message: ChatMessage }) {
  if (message.role === "system") {
    return (
      <Marker variant="separator">
        <MarkerContent>{plainMessageText(message)}</MarkerContent>
      </Marker>
    );
  }

  if (message.role === "user") {
    return (
      <div className="flex justify-end">
        <Bubble align="end" variant="default">
          <BubbleContent className="whitespace-pre-wrap">{plainMessageText(message)}</BubbleContent>
        </Bubble>
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-3 text-sm leading-relaxed">
      {message.parts.length === 0 ? (
        <LoadingStates.AssistantTyping />
      ) : (
        message.parts.map((part, index) => <MessagePart key={index} part={part} />)
      )}
    </div>
  );
}

function MessagePart({ part }: { readonly part: ChatMessagePart }) {
  switch (part.type) {
    case "text":
      return <Response>{part.text}</Response>;
    case "reasoning":
      return (
        <Reasoning isStreaming={part.state === "streaming"}>
          <ReasoningTrigger />
          <ReasoningContent>
            <Response>{part.text}</Response>
          </ReasoningContent>
        </Reasoning>
      );
    case "dynamic-tool": {
      const status: ToolStatus =
        part.state === "output-available"
          ? "complete"
          : part.state === "output-error"
            ? "error"
            : "running";
      return (
        <Tool defaultOpen={part.state === "output-error"}>
          <ToolHeader name={part.toolName} status={status} />
          <ToolContent>
            <ToolSection title="Input">
              <Response>{asCodeBlock(part.input)}</Response>
            </ToolSection>
            {part.state === "output-available" ? (
              <ToolSection title="Output">
                <Response>{asCodeBlock(part.output)}</Response>
              </ToolSection>
            ) : null}
            {part.state === "output-error" ? (
              <ToolSection title="Error">
                <p className="text-sm text-destructive">{part.errorText}</p>
              </ToolSection>
            ) : null}
          </ToolContent>
        </Tool>
      );
    }
    case "file":
      return part.mediaType.startsWith("image/") && part.url.startsWith("data:") ? (
        <Attachment orientation="vertical">
          <AttachmentMedia variant="image">
            <img alt="Attached file" src={part.url} />
          </AttachmentMedia>
          <AttachmentContent>
            <AttachmentTitle>Image</AttachmentTitle>
            <AttachmentDescription>{part.mediaType}</AttachmentDescription>
          </AttachmentContent>
        </Attachment>
      ) : (
        <Attachment>
          <AttachmentMedia>□</AttachmentMedia>
          <AttachmentContent>
            <AttachmentTitle>File</AttachmentTitle>
            <AttachmentDescription>{part.mediaType}</AttachmentDescription>
          </AttachmentContent>
        </Attachment>
      );
  }
}

function plainMessageText(message: ChatMessage): string {
  return message.parts
    .map((part) => (part.type === "text" || part.type === "reasoning" ? part.text : ""))
    .join("")
    .trim();
}

function statusLabel(status: ChatStatus): string {
  switch (status) {
    case "idle":
      return "Idle";
    case "connecting":
      return "Connecting";
    case "submitted":
      return "Submitted";
    case "streaming":
      return "Streaming";
    case "error":
      return "Error";
  }
}

function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

function asCodeBlock(value: unknown): string {
  const text = typeof value === "string" ? value : formatUnknown(value);
  const language = typeof value === "string" ? "" : "json";
  return `\`\`\`${language}\n${text}\n\`\`\``;
}

function formatUnknown(value: unknown): string {
  if (value === undefined) return "undefined";
  if (typeof value === "string") return value;
  try {
    return JSON.stringify(value, null, 2) ?? String(value);
  } catch {
    return String(value);
  }
}

export * as ConversationView from "./ConversationView.tsx";
