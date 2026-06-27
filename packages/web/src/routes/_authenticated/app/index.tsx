import { createFileRoute, useNavigate } from "@tanstack/react-router";

import { ConversationView } from "../../../chat/ConversationView.tsx";

export const Route = createFileRoute("/_authenticated/app/")({
  component: NewConversationRoute,
});

function NewConversationRoute() {
  const navigate = useNavigate();

  return (
    <ConversationView.View
      onConversationReady={(conversationId) =>
        navigate({
          to: "/app/conversations/$conversationId",
          params: { conversationId },
          replace: true,
        })
      }
    />
  );
}
