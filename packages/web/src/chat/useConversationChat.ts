import type { LiveMode } from "@durable-streams/client";
import { useCallback, useEffect, useMemo, useRef, useState, useSyncExternalStore } from "react";

import type { PersistedConversationMessage } from "./reducer.ts";
import {
  type ChatHistory,
  type ConversationClient,
  type SendMessageResult,
  Session,
  emptyChatSnapshot,
} from "./session.ts";
import type { ChatSnapshot } from "./types.ts";

export interface UseConversationChatOptions {
  readonly conversationId?: string | undefined;
  readonly history?: ChatHistory | undefined;
  readonly live?: LiveMode | undefined;
  readonly initialMessages?: ReadonlyArray<PersistedConversationMessage> | undefined;
  readonly client?: ConversationClient | undefined;
}

export interface UseConversationChatResult extends ChatSnapshot {
  readonly sendMessage: (message: string) => Promise<SendMessageResult>;
}

export interface UseLayoutConversationChatOptions extends Omit<
  UseConversationChatOptions,
  "conversationId"
> {
  readonly routeConversationId?: string | undefined;
  readonly onConversationReady?: ((conversationId: string) => void | Promise<void>) | undefined;
}

export interface UseLayoutConversationChatResult extends UseConversationChatResult {
  readonly reset: (conversationId?: string | undefined) => void;
}

interface CachedSession {
  readonly session: Session;
  refs: number;
  releaseTimer: ReturnType<typeof setTimeout> | undefined;
}

const DRAFT_SESSION_KEY = "__denora:draft-conversation__";
const RELEASE_DELAY_MS = 250;
const sessions = new Map<string, CachedSession>();
const emptySubscribe = () => () => {};

export function useConversationChat(
  options: UseConversationChatOptions = {},
): UseConversationChatResult {
  const key = sessionKey(options.conversationId);
  const session = useMemo(
    () =>
      retainConversationChatSession(key, {
        conversationId: options.conversationId,
        history: options.history,
        live: options.live,
        initialMessages: options.initialMessages,
        client: options.client,
      }),
    [
      key,
      options.conversationId,
      options.history,
      options.live,
      options.initialMessages,
      options.client,
    ],
  );

  useEffect(() => {
    session.start();
    return () => releaseConversationChatSession(key, session);
  }, [key, session]);

  const snapshot = useSyncExternalStore(
    session.subscribe ?? emptySubscribe,
    session.getSnapshot ?? (() => emptyChatSnapshot),
    () => emptyChatSnapshot,
  );

  return {
    ...snapshot,
    sendMessage: session.sendMessage.bind(session),
  };
}

export function useLayoutConversationChat(
  options: UseLayoutConversationChatOptions = {},
): UseLayoutConversationChatResult {
  const { routeConversationId, history, live, initialMessages, client, onConversationReady } =
    options;
  const activeSessionRef = useRef<Session | undefined>(undefined);
  const routeConversationIdForSessionRef = useRef<string | undefined>(routeConversationId);

  const makeSession = useCallback(
    (conversationId: string | undefined) =>
      new Session({
        conversationId,
        history,
        live,
        initialMessages,
        client,
        onConversationCreated: (createdConversationId, createdSession) => {
          if (activeSessionRef.current !== createdSession) return;
          void onConversationReady?.(createdConversationId);
        },
      }),
    [client, history, initialMessages, live, onConversationReady],
  );

  const [session, setSession] = useState(() => makeSession(routeConversationId));

  const replaceSession = useCallback(
    (conversationId: string | undefined) => {
      const next = makeSession(conversationId);
      routeConversationIdForSessionRef.current = conversationId;
      activeSessionRef.current = next;
      setSession(next);
    },
    [makeSession],
  );

  useEffect(() => {
    activeSessionRef.current = session;
  }, [session]);

  useEffect(() => {
    session.start();
    return () => session.dispose();
  }, [session]);

  const snapshot = useSyncExternalStore(
    session.subscribe ?? emptySubscribe,
    session.getSnapshot ?? (() => emptyChatSnapshot),
    () => emptyChatSnapshot,
  );

  useEffect(() => {
    if (routeConversationId === snapshot.conversationId) {
      routeConversationIdForSessionRef.current = routeConversationId;
      return;
    }

    const isDraftSessionBecomingDurable =
      routeConversationId === undefined &&
      routeConversationIdForSessionRef.current === undefined &&
      snapshot.conversationId !== undefined;
    if (isDraftSessionBecomingDurable) return;

    replaceSession(routeConversationId);
  }, [replaceSession, routeConversationId, snapshot.conversationId]);

  return {
    ...snapshot,
    sendMessage: session.sendMessage.bind(session),
    reset: replaceSession,
  };
}

export function retainConversationChatSession(
  key: string,
  options: UseConversationChatOptions = {},
): Session {
  const existing = sessions.get(key);
  if (existing !== undefined) {
    if (existing.releaseTimer !== undefined) {
      clearTimeout(existing.releaseTimer);
      existing.releaseTimer = undefined;
    }
    existing.refs += 1;
    return existing.session;
  }

  const session = new Session({
    ...options,
    onConversationCreated: (conversationId, createdSession) => {
      aliasConversationChatSession(sessionKey(conversationId), createdSession);
    },
  });
  sessions.set(key, { session, refs: 1, releaseTimer: undefined });
  return session;
}

export function releaseConversationChatSession(key: string, session: Session): void {
  const entry = sessions.get(key);
  if (entry === undefined || entry.session !== session) return;
  entry.refs = Math.max(0, entry.refs - 1);
  if (entry.releaseTimer !== undefined) clearTimeout(entry.releaseTimer);
  entry.releaseTimer = setTimeout(() => releaseUnreferencedAliases(session), RELEASE_DELAY_MS);
}

export function aliasConversationChatSession(key: string, session: Session): void {
  const existing = sessions.get(key);
  if (existing?.session === session) return;
  if (existing !== undefined) {
    existing.session.dispose();
  }
  sessions.set(key, { session, refs: 0, releaseTimer: undefined });
}

export function clearConversationChatSessionCache(): void {
  const unique = new Set([...sessions.values()].map((entry) => entry.session));
  sessions.clear();
  for (const session of unique) session.dispose();
}

function releaseUnreferencedAliases(session: Session): void {
  const related = [...sessions.entries()].filter(([, entry]) => entry.session === session);
  const refs = related.reduce((total, [, entry]) => total + entry.refs, 0);

  for (const [key, entry] of related) {
    if (entry.refs > 0) continue;
    if (entry.releaseTimer !== undefined) clearTimeout(entry.releaseTimer);
    sessions.delete(key);
  }

  if (refs === 0) session.dispose();
}

function sessionKey(conversationId: string | undefined): string {
  return conversationId ?? DRAFT_SESSION_KEY;
}

export * as UseConversationChat from "./useConversationChat.ts";
