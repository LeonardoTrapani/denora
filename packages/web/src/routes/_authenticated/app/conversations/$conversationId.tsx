import { createFileRoute } from "@tanstack/react-router";

export const Route = createFileRoute("/_authenticated/app/conversations/$conversationId")({
  component: ConversationRoute,
  pendingMs: Infinity,
});

function ConversationRoute() {
  return null;
}
