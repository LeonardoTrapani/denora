import { createFileRoute } from "@tanstack/react-router";

export const Route = createFileRoute("/_authenticated/app/")({
  component: NewConversationRoute,
  pendingMs: Infinity,
});

function NewConversationRoute() {
  return null;
}
