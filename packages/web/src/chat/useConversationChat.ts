import type { LiveMode } from "@durable-streams/client";
import { useEffect, useMemo, useSyncExternalStore } from "react";

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
