import {
  Attachment,
  AttachmentContent,
  AttachmentDescription,
  AttachmentMedia,
  AttachmentTitle,
} from "@denora/ui/components/attachment";
import { Avatar, AvatarFallback } from "@denora/ui/components/avatar";
import { Bubble, BubbleContent } from "@denora/ui/components/bubble";
import { Button } from "@denora/ui/components/button";
import { Marker, MarkerContent, MarkerIcon } from "@denora/ui/components/marker";
import {
  Message,
  MessageAvatar,
  MessageContent,
  MessageFooter,
  MessageHeader,
} from "@denora/ui/components/message";
import {
  MessageScroller,
  MessageScrollerButton,
  MessageScrollerContent,
  MessageScrollerItem,
  MessageScrollerProvider,
  MessageScrollerViewport,
} from "@denora/ui/components/message-scroller";
import { SidebarTrigger } from "@denora/ui/components/sidebar";
import { Textarea } from "@denora/ui/components/textarea";
import { useAtomSet } from "@effect/atom-react";
import { useEffect, useRef, useState, type FormEvent } from "react";

import { loadConversationsAtom } from "./atoms.ts";
import { LoadingStates } from "./LoadingStates.tsx";
import type { PersistedConversationMessage } from "./reducer.ts";
import type { ChatMessage, ChatMessagePart, ChatStatus } from "./types.ts";
import { useConversationChat } from "./useConversationChat.ts";

export interface Props {
  readonly conversationId?: string | undefined;
  readonly title?: string | null | undefined;
  readonly initialMessages?: ReadonlyArray<PersistedConversationMessage> | undefined;
  readonly onConversationReady?: ((conversationId: string) => void | Promise<void>) | undefined;
}

export function View({ conversationId, title, initialMessages, onConversationReady }: Props) {
  const chat = useConversationChat({ conversationId, history: 100, initialMessages });
  const [composerText, setComposerText] = useState("");
  const [sendPending, setSendPending] = useState(false);
  const [sendError, setSendError] = useState<Error | undefined>(undefined);
  const refreshConversations = useAtomSet(loadConversationsAtom, { mode: "promise" });
  const scrollAnchorRef = useRef<HTMLDivElement>(null);
  const routedConversationIdRef = useRef<string | undefined>(undefined);

  const canSend = composerText.trim().length > 0 && !sendPending;
  const showHistorySkeleton = !chat.historyReady && chat.messages.length === 0;

  useEffect(() => {
    scrollAnchorRef.current?.scrollIntoView({ block: "end" });
  }, [chat.messages, chat.status]);

  useEffect(() => {
    if (conversationId !== undefined || chat.conversationId === undefined || !onConversationReady) {
      return;
    }
    if (routedConversationIdRef.current === chat.conversationId) return;
    routedConversationIdRef.current = chat.conversationId;
    void onConversationReady(chat.conversationId);
  }, [chat.conversationId, conversationId, onConversationReady]);

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
      .catch((error: unknown) => {
        setComposerText(message);
        setSendError(toError(error));
      })
      .finally(() => setSendPending(false));
  };

  return (
    <div className="flex h-svh min-h-0 flex-col overflow-hidden">
      <header className="flex h-14 shrink-0 items-center justify-between border-b px-4">
        <div className="flex items-center gap-2">
          <SidebarTrigger />
          <h1 className="font-medium">{title ?? "Denora"}</h1>
        </div>
        <ChatStatusMarker status={chat.status} historyReady={chat.historyReady} />
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
                className="mx-auto w-full max-w-3xl gap-6 px-4 py-6"
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
                {!chat.historyReady && chat.messages.length > 0 ? (
                  <MessageScrollerItem messageId="syncing-history">
                    <SyncingHistory />
                  </MessageScrollerItem>
                ) : null}
                <div ref={scrollAnchorRef} className="h-px shrink-0" />
              </MessageScrollerContent>
            </MessageScrollerViewport>
            <MessageScrollerButton />
          </MessageScroller>
        </MessageScrollerProvider>

        <div className="shrink-0 border-t bg-background p-4">
          {chat.error ? <ErrorMarker message={chat.error.message} /> : null}
          {sendError ? <ErrorMarker message={sendError.message} /> : null}
          <form className="mx-auto flex w-full max-w-3xl items-end gap-2" onSubmit={send}>
            <Textarea
              disabled={sendPending}
              placeholder="Message Denora..."
              value={composerText}
              onChange={(event) => setComposerText(event.currentTarget.value)}
              onKeyDown={(event) => {
                if (event.key === "Enter" && !event.shiftKey) {
                  event.preventDefault();
                  event.currentTarget.form?.requestSubmit();
                }
              }}
            />
            <Button disabled={!canSend} type="submit">
              {sendPending ? "Sending" : "Send"}
            </Button>
          </form>
        </div>
      </section>
    </div>
  );
}

function ChatStatusMarker({
  status,
  historyReady,
}: {
  readonly status: ChatStatus;
  readonly historyReady: boolean;
}) {
  return (
    <Marker role={status === "streaming" || status === "connecting" ? "status" : undefined}>
      <MarkerIcon>
        {status === "streaming" || status === "submitted" || status === "connecting" ? "●" : "○"}
      </MarkerIcon>
      <MarkerContent>{historyReady ? statusLabel(status) : "Syncing history"}</MarkerContent>
    </Marker>
  );
}

function EmptyChat() {
  return (
    <Marker variant="separator">
      <MarkerContent>Start a conversation with your Denora agent.</MarkerContent>
    </Marker>
  );
}

function SyncingHistory() {
  return (
    <Marker variant="separator">
      <MarkerContent>Syncing history...</MarkerContent>
    </Marker>
  );
}

function ErrorMarker({ message }: { readonly message: string }) {
  return (
    <Marker>
      <MarkerIcon>!</MarkerIcon>
      <MarkerContent>{message}</MarkerContent>
    </Marker>
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

  const isUser = message.role === "user";
  return (
    <Message align={isUser ? "end" : "start"}>
      {!isUser ? (
        <MessageAvatar>
          <Avatar>
            <AvatarFallback>DN</AvatarFallback>
          </Avatar>
        </MessageAvatar>
      ) : null}
      <MessageContent>
        <MessageHeader>{isUser ? "You" : "Denora"}</MessageHeader>
        <Bubble variant={isUser ? "default" : "ghost"}>
          <BubbleContent>
            {message.parts.length === 0 && !isUser ? (
              <LoadingStates.AssistantTyping />
            ) : (
              message.parts.map((part, index) => <MessagePart key={index} part={part} />)
            )}
          </BubbleContent>
        </Bubble>
        {message.metadata?.model ? (
          <MessageFooter>
            {message.metadata.model.provider}/{message.metadata.model.id}
          </MessageFooter>
        ) : null}
      </MessageContent>
    </Message>
  );
}

function MessagePart({ part }: { readonly part: ChatMessagePart }) {
  switch (part.type) {
    case "text":
      return <p className="whitespace-pre-wrap">{part.text}</p>;
    case "reasoning":
      return (
        <details open={part.state === "streaming"}>
          <summary>Reasoning</summary>
          <p className="whitespace-pre-wrap">{part.text}</p>
        </details>
      );
    case "dynamic-tool":
      return (
        <Marker role={part.state === "input-available" ? "status" : undefined}>
          <MarkerIcon>⚙</MarkerIcon>
          <MarkerContent>
            Tool: {part.toolName}
            <pre className="whitespace-pre-wrap">{formatUnknown(part.input)}</pre>
            {part.state === "output-available" ? (
              <pre className="whitespace-pre-wrap">{formatUnknown(part.output)}</pre>
            ) : null}
            {part.state === "output-error" ? <span>{part.errorText}</span> : null}
          </MarkerContent>
        </Marker>
      );
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
