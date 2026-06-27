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
import { Link, Outlet, createFileRoute, useNavigate, useRouterState } from "@tanstack/react-router";
import * as Effect from "effect/Effect";
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult";
import * as Atom from "effect/unstable/reactivity/Atom";

import { loadConversationsAtom, type ConversationSummary } from "../../chat/atoms.ts";
import { LoadingStates } from "../../chat/LoadingStates.tsx";
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
  const pathname = useRouterState({ select: (state) => state.location.pathname });
  const [signOutResult, signOut] = useAtom(signOutAtom, { mode: "promise" });
  const refreshedConversations = useAtomValue(loadConversationsAtom);
  const conversations = AsyncResult.isSuccess(refreshedConversations)
    ? refreshedConversations.value
    : loaderConversations;

  return (
    <SidebarProvider>
      <Sidebar collapsible="offcanvas">
        <SidebarHeader>
          <SidebarMenu>
            <SidebarMenuItem>
              <SidebarMenuButton isActive={pathname === "/app"} render={<Link to="/app" />}>
                <span>Denora</span>
              </SidebarMenuButton>
            </SidebarMenuItem>
          </SidebarMenu>
          <Button type="button" variant="outline" onClick={() => void navigate({ to: "/app" })}>
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
                    isActive={pathname === conversationPath(conversation.id)}
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

function conversationPath(conversationId: string): string {
  return `/app/conversations/${encodeURIComponent(conversationId)}`;
}
