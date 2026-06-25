import { useEffect, useMemo, useSyncExternalStore } from "react";

import { ConversationChatSession, type ConversationChatSessionOptions } from "./session.ts";
import type { ChatSnapshot } from "./types.ts";

const emptySnapshot: ChatSnapshot = {
  conversationId: undefined,
  messages: [],
  status: "idle",
  historyReady: true,
  error: undefined,
};

export interface UseConversationChatResult extends ChatSnapshot {
  sendMessage(message: string): Promise<void>;
}

export function useConversationChat(
  options: ConversationChatSessionOptions = {},
): UseConversationChatResult {
  const session = useMemo(
    () => new ConversationChatSession(options),
    [options.conversationId, options.history, options.live],
  );
  useEffect(() => {
    session.start();
    return () => session.dispose();
  }, [session]);
  const snapshot = useSyncExternalStore(
    session.subscribe,
    session.getSnapshot,
    () => emptySnapshot,
  );
  return {
    ...snapshot,
    sendMessage: session.sendMessage.bind(session),
  };
}

export * as UseConversationChat from "./use-conversation-chat.ts";
