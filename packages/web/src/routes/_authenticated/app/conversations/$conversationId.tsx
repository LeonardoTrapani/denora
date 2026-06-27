import { createFileRoute } from "@tanstack/react-router";

import { ConversationView } from "../../../../chat/ConversationView.tsx";

export const Route = createFileRoute("/_authenticated/app/conversations/$conversationId")({
  component: ConversationRoute,
});

function ConversationRoute() {
  const { conversationId } = Route.useParams();

  return <ConversationView.View conversationId={conversationId} />;
}
