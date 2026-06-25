import { Button } from "@denora/ui/components/button";
import { Textarea } from "@denora/ui/components/textarea";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { createFileRoute } from "@tanstack/react-router";
import { useEffect, useState, type FormEvent } from "react";

import { getAuthClient } from "../../auth-client.ts";
import type { ChatMessagePart } from "../../chat/types.ts";
import { useConversationChat } from "../../chat/use-conversation-chat.ts";
import { apiEffect, runApi } from "../../lib/api.ts";

export const Route = createFileRoute("/_authenticated/app")({
  component: AppHome,
});

function AppHome() {
  const { auth } = Route.useRouteContext();
  const user = auth.user;
  const queryClient = useQueryClient();
  const [selectedConversationId, setSelectedConversationId] = useState<string | undefined>();
  const chat = useConversationChat({ conversationId: selectedConversationId, history: 100 });
  const [input, setInput] = useState("");
  const conversationsQuery = useQuery({
    queryKey: ["conversations"],
    queryFn: () =>
      runApi(
        apiEffect((client) => client.listConversations()),
        { span: "chat.list" },
      ),
  });
  const signOutMutation = useMutation({
    mutationFn: async () => {
      getAuthClient().signOut();
    },
  });
  const sendMutation = useMutation({
    mutationFn: (message: string) => chat.sendMessage(message),
  });

  const send = (event: FormEvent) => {
    event.preventDefault();
    const message = input.trim();
    if (message.length === 0 || sendMutation.isPending) return;
    setInput("");
    sendMutation.mutate(message);
  };

  useEffect(() => {
    if (chat.conversationId === undefined || chat.conversationId === selectedConversationId) return;
    setSelectedConversationId(chat.conversationId);
    void queryClient.invalidateQueries({ queryKey: ["conversations"] });
  }, [chat.conversationId, queryClient, selectedConversationId]);

  return (
    <main className="mx-auto flex min-h-screen w-full max-w-5xl flex-col px-4 py-6">
      <header className="mb-6 flex items-center justify-between gap-3">
        <div>
          <p className="text-sm text-muted-foreground">Signed in as {user.email}</p>
          <h1 className="text-2xl font-semibold tracking-tight">Denora</h1>
        </div>
        <Button
          disabled={signOutMutation.isPending}
          size="sm"
          type="button"
          variant="outline"
          onClick={() => signOutMutation.mutate()}
        >
          {signOutMutation.isPending ? "Signing out..." : "Sign out"}
        </Button>
      </header>

      <div className="grid min-h-0 flex-1 gap-4 md:grid-cols-[16rem_minmax(0,1fr)]">
        <aside className="rounded-3xl border bg-background p-4">
          <div className="mb-3 flex items-center justify-between gap-2">
            <h2 className="text-sm font-semibold">Conversations</h2>
            <Button
              size="sm"
              type="button"
              variant="outline"
              onClick={() => setSelectedConversationId(undefined)}
            >
              New
            </Button>
          </div>
          {conversationsQuery.error ? (
            <p className="text-sm text-destructive">{conversationsQuery.error.message}</p>
          ) : null}
          <div className="space-y-2">
            {(conversationsQuery.data ?? []).map((conversation) => (
              <Button
                key={conversation.id}
                className="w-full justify-start text-left"
                type="button"
                variant={conversation.id === selectedConversationId ? "default" : "outline"}
                onClick={() => setSelectedConversationId(conversation.id)}
              >
                <span className="truncate">{conversation.title ?? conversation.id}</span>
              </Button>
            ))}
            {conversationsQuery.isLoading ? (
              <p className="text-sm text-muted-foreground">Loading conversations...</p>
            ) : null}
          </div>
        </aside>

        <section className="flex min-h-0 flex-1 flex-col rounded-3xl border bg-background p-4">
          <div className="mb-3 flex items-center justify-between text-xs text-muted-foreground">
            <span>
              Status: {chat.historyReady ? chat.status : `loading history (${chat.status})`}
            </span>
            {chat.conversationId ? (
              <span>{chat.conversationId}</span>
            ) : (
              <span>New conversation</span>
            )}
          </div>

          <div className="min-h-0 flex-1 space-y-3 overflow-y-auto pb-4" aria-live="polite">
            {chat.messages.length === 0 ? (
              <p className="rounded-2xl bg-muted/50 p-4 text-sm text-muted-foreground">
                Start a conversation with your Denora agent.
              </p>
            ) : (
              chat.messages.map((message) => (
                <article
                  key={message.id}
                  className={
                    message.role === "user"
                      ? "ml-auto max-w-[85%] rounded-2xl bg-primary px-4 py-3 text-primary-foreground"
                      : "mr-auto max-w-[85%] rounded-2xl bg-muted px-4 py-3"
                  }
                >
                  <div className="mb-1 text-xs font-medium opacity-70">{message.role}</div>
                  <div className="space-y-2 text-sm leading-6">
                    {message.parts.map((part, index) => (
                      <MessagePart key={index} part={part} />
                    ))}
                  </div>
                </article>
              ))
            )}
          </div>

          {chat.error ? (
            <p className="mb-2 text-sm text-destructive">{chat.error.message}</p>
          ) : null}
          {sendMutation.error ? (
            <p className="mb-2 text-sm text-destructive">{sendMutation.error.message}</p>
          ) : null}

          <form className="flex gap-2" onSubmit={send}>
            <Textarea
              className="min-h-12 flex-1"
              placeholder="Message Denora..."
              value={input}
              onChange={(event) => setInput(event.currentTarget.value)}
              onKeyDown={(event) => {
                if (event.key === "Enter" && !event.shiftKey) {
                  event.preventDefault();
                  event.currentTarget.form?.requestSubmit();
                }
              }}
            />
            <Button disabled={input.trim().length === 0 || sendMutation.isPending} type="submit">
              Send
            </Button>
          </form>
        </section>
      </div>
    </main>
  );
}

function MessagePart({ part }: { readonly part: ChatMessagePart }) {
  switch (part.type) {
    case "text":
      return <p className="whitespace-pre-wrap">{part.text}</p>;
    case "reasoning":
      return (
        <details className="rounded-xl border px-3 py-2" open={part.state === "streaming"}>
          <summary className="cursor-pointer text-xs font-medium">Reasoning</summary>
          <p className="mt-2 whitespace-pre-wrap">{part.text}</p>
        </details>
      );
    case "dynamic-tool":
      return (
        <div className="rounded-xl border px-3 py-2">
          <p className="text-xs font-medium">Tool: {part.toolName}</p>
          <pre className="mt-2 overflow-x-auto whitespace-pre-wrap text-xs">
            {formatUnknown(part.input)}
          </pre>
          {part.state === "output-available" ? (
            <pre className="mt-2 overflow-x-auto whitespace-pre-wrap text-xs">
              {formatUnknown(part.output)}
            </pre>
          ) : null}
          {part.state === "output-error" ? (
            <p className="mt-2 text-xs text-destructive">{part.errorText}</p>
          ) : null}
        </div>
      );
    case "file":
      return part.mediaType.startsWith("image/") && part.url.startsWith("data:") ? (
        <img
          alt="Attached file"
          className="max-h-80 rounded-xl border object-contain"
          src={part.url}
        />
      ) : (
        <p className="text-xs">File: {part.mediaType}</p>
      );
  }
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
