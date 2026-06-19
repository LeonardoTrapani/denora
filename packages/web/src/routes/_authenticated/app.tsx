import { Alert, AlertDescription } from "@denora/ui/components/alert";
import { Avatar, AvatarFallback } from "@denora/ui/components/avatar";
import { Badge } from "@denora/ui/components/badge";
import { Button } from "@denora/ui/components/button";
import { Separator } from "@denora/ui/components/separator";
import { Textarea } from "@denora/ui/components/textarea";
import { useMutation } from "@tanstack/react-query";
import { createFileRoute } from "@tanstack/react-router";
import { useEffect, useRef, useState } from "react";

import { getAuthClient } from "../../auth-client.ts";
import { AgentStream } from "../../lib/agent-stream.ts";

export const Route = createFileRoute("/_authenticated/app")({
  component: AppHome,
});

type ChatRole = "assistant" | "user";

interface ChatMessage {
  readonly id: string;
  readonly role: ChatRole;
  readonly content: string;
}

interface ChatThread {
  readonly id: string;
  readonly title: string;
  readonly createdAt: string;
  readonly updatedAt: string;
  readonly messages: ReadonlyArray<ChatMessage>;
}

const storageKey = (userId: string) => `denora.chat.threads.v1.${userId}`;

const nowIso = () => new Date().toISOString();

const newId = (prefix: string) => `${prefix}_${crypto.randomUUID().replaceAll("-", "")}`;

const createThread = (): ChatThread => {
  const createdAt = nowIso();
  return {
    id: newId("thread"),
    title: "New thread",
    createdAt,
    updatedAt: createdAt,
    messages: [],
  };
};

const titleFromMessage = (message: string) => {
  const title = message.trim().replace(/\s+/g, " ");
  return title.length > 44 ? `${title.slice(0, 41)}...` : title;
};

const parseStoredThreads = (value: string | null): ReadonlyArray<ChatThread> | undefined => {
  if (value === null) return undefined;

  try {
    const parsed = JSON.parse(value) as unknown;
    if (!Array.isArray(parsed)) return undefined;
    return parsed.filter(isChatThread);
  } catch {
    return undefined;
  }
};

const isChatThread = (value: unknown): value is ChatThread => {
  if (typeof value !== "object" || value === null) return false;
  const thread = value as Partial<ChatThread>;
  return (
    typeof thread.id === "string" &&
    typeof thread.title === "string" &&
    typeof thread.createdAt === "string" &&
    typeof thread.updatedAt === "string" &&
    Array.isArray(thread.messages) &&
    thread.messages.every(isChatMessage)
  );
};

const isChatMessage = (value: unknown): value is ChatMessage => {
  if (typeof value !== "object" || value === null) return false;
  const message = value as Partial<ChatMessage>;
  return (
    typeof message.id === "string" &&
    (message.role === "assistant" || message.role === "user") &&
    typeof message.content === "string"
  );
};

function AppHome() {
  const { auth } = Route.useRouteContext();
  const user = auth.user;
  const userStorageKey = storageKey(user.id);
  const userInitial = (user.name || user.email || "D").slice(0, 1).toUpperCase();
  const agentInitial = "D";
  const abortRef = useRef<AbortController | null>(null);
  const [threads, setThreads] = useState<ReadonlyArray<ChatThread>>([]);
  const [activeThreadId, setActiveThreadId] = useState<string | null>(null);
  const [draft, setDraft] = useState("");
  const [isLoaded, setIsLoaded] = useState(false);
  const [streamingThreadId, setStreamingThreadId] = useState<string | null>(null);
  const [streamError, setStreamError] = useState<string | null>(null);

  const signOutMutation = useMutation({
    mutationFn: async () => {
      getAuthClient().signOut();
    },
  });

  useEffect(() => {
    const stored = parseStoredThreads(localStorage.getItem(userStorageKey));
    const loadedThreads = stored && stored.length > 0 ? stored : [createThread()];
    setThreads(loadedThreads);
    setActiveThreadId(loadedThreads[0]?.id ?? null);
    setIsLoaded(true);
  }, [userStorageKey]);

  useEffect(() => {
    if (!isLoaded) return;
    localStorage.setItem(userStorageKey, JSON.stringify(threads));
  }, [isLoaded, threads, userStorageKey]);

  useEffect(
    () => () => {
      abortRef.current?.abort();
    },
    [],
  );

  const activeThread = threads.find((thread) => thread.id === activeThreadId) ?? threads[0];
  const isStreaming = streamingThreadId !== null;
  const canSend = draft.trim().length > 0 && activeThread !== undefined && !isStreaming;

  const startNewThread = () => {
    const thread = createThread();
    setThreads((current) => [thread, ...current]);
    setActiveThreadId(thread.id);
    setDraft("");
    setStreamError(null);
  };

  const sendMessage = async () => {
    if (!canSend || activeThread === undefined) return;

    const message = draft.trim();
    const threadId = activeThread.id;
    const userMessage: ChatMessage = { id: newId("msg"), role: "user", content: message };
    const assistantMessage: ChatMessage = { id: newId("msg"), role: "assistant", content: "" };
    const controller = new AbortController();
    let receivedChunk = false;

    abortRef.current = controller;
    setDraft("");
    setStreamError(null);
    setStreamingThreadId(threadId);
    setThreads((current) =>
      current.map((thread) =>
        thread.id === threadId
          ? {
              ...thread,
              title: thread.messages.length === 0 ? titleFromMessage(message) : thread.title,
              updatedAt: nowIso(),
              messages: [...thread.messages, userMessage, assistantMessage],
            }
          : thread,
      ),
    );

    try {
      await AgentStream.streamAgentMessage({
        agentId: user.id,
        threadId,
        message,
        signal: controller.signal,
        onChunk: (chunk) => {
          if (chunk.length > 0) receivedChunk = true;
          setThreads((current) =>
            current.map((thread) =>
              thread.id === threadId
                ? {
                    ...thread,
                    updatedAt: nowIso(),
                    messages: thread.messages.map((item) =>
                      item.id === assistantMessage.id
                        ? { ...item, content: `${item.content}${chunk}` }
                        : item,
                    ),
                  }
                : thread,
            ),
          );
        },
      });
      if (!receivedChunk) {
        setStreamError("The assistant stream completed without text.");
        setThreads((current) =>
          current.map((thread) =>
            thread.id === threadId
              ? {
                  ...thread,
                  messages: thread.messages.map((item) =>
                    item.id === assistantMessage.id
                      ? { ...item, content: "I didn't receive any response text. Try again." }
                      : item,
                  ),
                }
              : thread,
          ),
        );
      }
    } catch (error) {
      if (error instanceof DOMException && error.name === "AbortError") return;
      const messageText = error instanceof Error ? error.message : "The stream failed";
      setStreamError(messageText);
      setThreads((current) =>
        current.map((thread) =>
          thread.id === threadId
            ? {
                ...thread,
                messages: thread.messages.map((item) =>
                  item.id === assistantMessage.id
                    ? { ...item, content: "I couldn't finish that response. Try again." }
                    : item,
                ),
              }
            : thread,
        ),
      );
    } finally {
      abortRef.current = null;
      setStreamingThreadId(null);
    }
  };

  return (
    <main className="min-h-svh bg-[radial-gradient(circle_at_top_left,theme(colors.primary/18),transparent_32rem),linear-gradient(135deg,theme(colors.background),theme(colors.muted/55))] text-foreground">
      <div className="mx-auto flex min-h-svh w-full max-w-7xl flex-col gap-4 p-3 md:grid md:grid-cols-[20rem_1fr] md:p-5">
        <aside className="rounded-3xl border bg-background/80 p-3 shadow-sm backdrop-blur md:min-h-[calc(100svh-2.5rem)]">
          <div className="flex items-center justify-between gap-3">
            <div>
              <Badge variant="secondary">Denora</Badge>
              <h1 className="mt-3 text-xl font-semibold tracking-tight">Threads</h1>
            </div>
            <Button size="sm" type="button" onClick={startNewThread}>
              New
            </Button>
          </div>

          <div className="mt-4 flex gap-2 overflow-x-auto pb-1 md:block md:space-y-2 md:overflow-visible md:pb-0">
            {threads.map((thread) => (
              <button
                className={`min-w-56 rounded-2xl border p-3 text-left transition md:w-full ${
                  thread.id === activeThread?.id
                    ? "border-primary/40 bg-primary/10 shadow-sm"
                    : "border-border bg-muted/35 hover:bg-muted"
                }`}
                key={thread.id}
                type="button"
                onClick={() => setActiveThreadId(thread.id)}
              >
                <div className="line-clamp-1 text-sm font-medium">{thread.title}</div>
                <div className="mt-1 text-xs text-muted-foreground">
                  {thread.messages.length === 0
                    ? "No messages yet"
                    : `${thread.messages.length} messages`}
                </div>
              </button>
            ))}
          </div>

          <Separator className="my-4" />

          <div className="flex items-center gap-3">
            <Avatar>
              <AvatarFallback>{userInitial}</AvatarFallback>
            </Avatar>
            <div className="min-w-0 flex-1">
              <div className="truncate text-sm font-medium">{user.email}</div>
              <div className="text-xs text-muted-foreground">Signed in</div>
            </div>
          </div>
          <Button
            className="mt-3 w-full"
            disabled={signOutMutation.isPending}
            size="sm"
            type="button"
            variant="outline"
            onClick={() => signOutMutation.mutate()}
          >
            {signOutMutation.isPending ? "Signing out..." : "Sign out"}
          </Button>
        </aside>

        <section className="flex min-h-[70svh] flex-col overflow-hidden rounded-3xl border bg-background/90 shadow-sm backdrop-blur md:min-h-[calc(100svh-2.5rem)]">
          <header className="border-b p-4 md:p-5">
            <div className="flex items-start justify-between gap-3">
              <div>
                <Badge variant="outline">Personal agent</Badge>
                <h2 className="mt-2 text-2xl font-semibold tracking-tight">
                  {activeThread?.title ?? "Chat"}
                </h2>
              </div>
              {isStreaming ? <Badge>Streaming</Badge> : null}
            </div>
          </header>

          <div className="flex-1 space-y-4 overflow-y-auto p-4 md:p-6">
            {activeThread === undefined || activeThread.messages.length === 0 ? (
              <div className="grid min-h-72 place-items-center rounded-3xl border border-dashed bg-muted/25 p-8 text-center">
                <div className="max-w-md">
                  <div className="text-2xl font-semibold tracking-tight">
                    Start with a plain request.
                  </div>
                  <p className="mt-2 text-sm text-muted-foreground">
                    This page is intentionally just chat for now: create threads, switch between
                    them, and watch the assistant response stream in.
                  </p>
                </div>
              </div>
            ) : (
              activeThread.messages.map((message) => (
                <div
                  className={`flex gap-3 ${message.role === "user" ? "justify-end" : "justify-start"}`}
                  key={message.id}
                >
                  {message.role === "assistant" ? (
                    <Avatar className="mt-1">
                      <AvatarFallback>{agentInitial}</AvatarFallback>
                    </Avatar>
                  ) : null}
                  <div
                    className={`max-w-[82%] rounded-3xl px-4 py-3 text-sm leading-6 whitespace-pre-wrap ${
                      message.role === "user"
                        ? "bg-primary text-primary-foreground"
                        : "border bg-muted/45"
                    }`}
                  >
                    {message.content.length > 0 ? message.content : "Thinking..."}
                  </div>
                  {message.role === "user" ? (
                    <Avatar className="mt-1">
                      <AvatarFallback>{userInitial}</AvatarFallback>
                    </Avatar>
                  ) : null}
                </div>
              ))
            )}
          </div>

          <div className="border-t bg-background/95 p-3 md:p-4">
            {streamError !== null ? (
              <Alert className="mb-3" variant="destructive">
                <AlertDescription>{streamError}</AlertDescription>
              </Alert>
            ) : null}
            {signOutMutation.error instanceof Error ? (
              <Alert className="mb-3" variant="destructive">
                <AlertDescription>{signOutMutation.error.message}</AlertDescription>
              </Alert>
            ) : null}
            <form
              className="flex flex-col gap-3 md:flex-row md:items-end"
              onSubmit={(event) => {
                event.preventDefault();
                void sendMessage();
              }}
            >
              <Textarea
                className="max-h-40 min-h-20 flex-1 bg-muted/60"
                disabled={!isLoaded || isStreaming}
                placeholder="Message your agent..."
                value={draft}
                onChange={(event) => setDraft(event.target.value)}
                onKeyDown={(event) => {
                  if (event.key === "Enter" && !event.shiftKey) {
                    event.preventDefault();
                    void sendMessage();
                  }
                }}
              />
              <Button className="md:w-28" disabled={!canSend} size="lg" type="submit">
                {isStreaming ? "Sending" : "Send"}
              </Button>
            </form>
          </div>
        </section>
      </div>
    </main>
  );
}
