import { createFileRoute } from "@tanstack/react-router";
import * as Effect from "effect/Effect";

import { ConversationView } from "../../../../chat/ConversationView.tsx";
import { LoadingStates } from "../../../../chat/LoadingStates.tsx";
import { Api } from "../../../../lib/api.ts";

export const Route = createFileRoute("/_authenticated/app/conversations/$conversationId")({
  loader: ({ params }) =>
    Api.runApi(
      Api.apiEffect((client) =>
        Effect.all({
          conversations: client.listConversations(),
          messages: client.listConversationMessages({
            params: { conversationId: params.conversationId },
          }),
        }),
      ),
      { span: "routes.app.conversation" },
    ).then(({ conversations, messages }) => ({
      conversation: conversations.find((conversation) => conversation.id === params.conversationId),
      messages,
    })),
  pendingComponent: LoadingStates.ConversationPanelSkeleton,
  component: ConversationRoute,
});

function ConversationRoute() {
  const { conversationId } = Route.useParams();
  const { conversation, messages } = Route.useLoaderData();

  return (
    <ConversationView.View
      conversationId={conversationId}
      initialMessages={messages}
      title={conversation?.title}
    />
  );
}
