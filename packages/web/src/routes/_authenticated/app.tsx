import { Button } from "@denora/ui/components/button";
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarInset,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarProvider,
} from "@denora/ui/components/sidebar";
import { useAtom, useAtomValue } from "@effect/atom-react";
import { Link, Outlet, createFileRoute, useMatchRoute, useNavigate } from "@tanstack/react-router";
import { useCallback } from "react";
import * as Effect from "effect/Effect";
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult";
import * as Atom from "effect/unstable/reactivity/Atom";

import { loadConversationsAtom, type ConversationSummary } from "../../chat/atoms.ts";
import { ConversationView } from "../../chat/ConversationView.tsx";
import { LoadingStates } from "../../chat/LoadingStates.tsx";
import { useLayoutConversationChat } from "../../chat/useConversationChat.ts";
import { Api } from "../../lib/api.ts";
import { Auth } from "../../lib/Auth.ts";

export const Route = createFileRoute("/_authenticated/app")({
  loader: () =>
    Api.runApi(
      Api.apiEffect((client) => client.listConversations()),
      { span: "routes.app.listConversations" },
    ),
  pendingComponent: LoadingStates.AppShellSkeleton,
  component: AppLayout,
});

const signOutAtom = Atom.fn<void>()(() =>
  Effect.sync(() => {
    Auth.signOut();
  }),
);

function AppLayout() {
  const loaderConversations = Route.useLoaderData();
  const { auth } = Route.useRouteContext();
  const navigate = useNavigate();
  const matchRoute = useMatchRoute();
  const conversationMatch = matchRoute({ to: "/app/conversations/$conversationId" });
  const routeConversationId =
    conversationMatch === false ? undefined : conversationMatch.conversationId;
  const [signOutResult, signOut] = useAtom(signOutAtom, { mode: "promise" });
  const refreshedConversations = useAtomValue(loadConversationsAtom);
  const conversations = AsyncResult.isSuccess(refreshedConversations)
    ? refreshedConversations.value
    : loaderConversations;
  const handleConversationReady = useCallback(
    (conversationId: string) =>
      navigate({
        to: "/app/conversations/$conversationId",
        params: { conversationId },
        replace: true,
      }),
    [navigate],
  );
  const chat = useLayoutConversationChat({
    routeConversationId,
    history: 100,
    onConversationReady: handleConversationReady,
  });
  const activeConversationId = routeConversationId ?? chat.conversationId;
  const activeConversation = conversations.find(
    (conversation) => conversation.id === activeConversationId,
  );

  const startNewConversation = () => {
    if (routeConversationId === undefined) chat.reset();
    void navigate({ to: "/app" });
  };

  return (
    <SidebarProvider>
      <Sidebar collapsible="offcanvas">
        <SidebarHeader>
          <SidebarMenu>
            <SidebarMenuItem>
              <SidebarMenuButton
                isActive={routeConversationId === undefined}
                render={<Link to="/app" />}
              >
                <span>Denora</span>
              </SidebarMenuButton>
            </SidebarMenuItem>
          </SidebarMenu>
          <Button type="button" variant="outline" onClick={startNewConversation}>
            New conversation
          </Button>
        </SidebarHeader>
        <SidebarContent>
          <SidebarGroup>
            <SidebarGroupLabel>Conversations</SidebarGroupLabel>
            <SidebarGroupContent>
              <SidebarMenu>
                {conversations.map((conversation) => (
                  <ConversationItem
                    conversation={conversation}
                    isActive={routeConversationId === conversation.id}
                    key={conversation.id}
                    to="/app/conversations/$conversationId"
                  />
                ))}
              </SidebarMenu>
            </SidebarGroupContent>
          </SidebarGroup>
        </SidebarContent>
        <SidebarFooter>
          <SidebarMenu>
            <SidebarMenuItem>
              <SidebarMenuButton>
                <span>{auth.user.email}</span>
              </SidebarMenuButton>
            </SidebarMenuItem>
            <SidebarMenuItem>
              <SidebarMenuButton disabled={signOutResult.waiting} onClick={() => void signOut()}>
                <span>{signOutResult.waiting ? "Signing out..." : "Sign out"}</span>
              </SidebarMenuButton>
            </SidebarMenuItem>
          </SidebarMenu>
        </SidebarFooter>
      </Sidebar>

      <SidebarInset className="min-h-0 min-w-0 overflow-hidden">
        <ConversationView.View chat={chat} title={activeConversation?.title} />
        <Outlet />
      </SidebarInset>
    </SidebarProvider>
  );
}

function ConversationItem({
  conversation,
  isActive,
  to,
}: {
  readonly conversation: ConversationSummary;
  readonly isActive: boolean;
  readonly to: "/app/conversations/$conversationId";
}) {
  return (
    <SidebarMenuItem>
      <SidebarMenuButton
        isActive={isActive}
        render={<Link params={{ conversationId: conversation.id }} to={to} />}
      >
        <span>{conversation.title ?? "TODO(conversation)"}</span>
      </SidebarMenuButton>
    </SidebarMenuItem>
  );
}
