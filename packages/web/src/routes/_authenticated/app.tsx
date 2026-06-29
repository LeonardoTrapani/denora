import { Kbd } from "@denora/ui/components/kbd";
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
  SidebarRail,
} from "@denora/ui/components/sidebar";
import { useAtom, useAtomValue } from "@effect/atom-react";
import { IconDots, IconPencilPlus, IconSearch } from "@tabler/icons-react";
import { Link, Outlet, createFileRoute, useNavigate, useRouterState } from "@tanstack/react-router";
import { useCallback, useEffect, useState } from "react";
import * as Effect from "effect/Effect";
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult";
import * as Atom from "effect/unstable/reactivity/Atom";

import { ConversationCommandMenu } from "../../components/ConversationCommandMenu.tsx";
import { NavUser } from "../../components/NavUser.tsx";
import { loadConversationsAtom, type ConversationSummary } from "../../chat/atoms.ts";
import { ConversationView } from "../../chat/ConversationView.tsx";
import { LoadingStates } from "../../chat/LoadingStates.tsx";
import { useLayoutConversationChat } from "../../chat/useConversationChat.ts";
import { Api } from "../../lib/api.ts";
import { Auth } from "../../lib/Auth.ts";

// Keep the sidebar focused: only the most recent threads live there, the long
// tail is reachable through the ⌘K palette so the list never grows unbounded.
const RECENT_LIMIT = 7;

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
  const routeConversationId = useRouterState({
    select: (state) => conversationIdFromPathname(state.location.pathname),
  });
  const [signOutResult, signOut] = useAtom(signOutAtom, { mode: "promise" });
  const refreshedConversations = useAtomValue(loadConversationsAtom);
  const conversations = AsyncResult.isSuccess(refreshedConversations)
    ? refreshedConversations.value
    : loaderConversations;
  const [commandOpen, setCommandOpen] = useState(false);

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
    history: "all",
    onConversationReady: handleConversationReady,
  });
  const activeConversationId = routeConversationId ?? chat.conversationId;
  const activeConversation = conversations.find(
    (conversation) => conversation.id === activeConversationId,
  );

  const sortedConversations = [...conversations].sort(
    (a, b) => Date.parse(b.updatedAt) - Date.parse(a.updatedAt),
  );
  const recentConversations = sortedConversations.slice(0, RECENT_LIMIT);
  const hasMore = sortedConversations.length > RECENT_LIMIT;

  const startNewConversation = useCallback(() => {
    if (routeConversationId === undefined) chat.reset();
    void navigate({ to: "/app" });
  }, [chat, navigate, routeConversationId]);

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "k" && (event.metaKey || event.ctrlKey)) {
        event.preventDefault();
        setCommandOpen((open) => !open);
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, []);

  return (
    <SidebarProvider>
      <Sidebar collapsible="icon">
        <SidebarHeader className="px-4 pt-4 pb-3 group-data-[collapsible=icon]:px-2">
          <Link
            to="/app"
            className="font-heading text-xl font-semibold tracking-tight text-sidebar-foreground group-data-[collapsible=icon]:hidden"
          >
            Denora
          </Link>
        </SidebarHeader>

        <SidebarContent>
          <SidebarGroup>
            <SidebarGroupContent>
              <SidebarMenu>
                <SidebarMenuItem>
                  <SidebarMenuButton
                    isActive={routeConversationId === undefined}
                    onClick={startNewConversation}
                    tooltip="New chat"
                  >
                    <IconPencilPlus />
                    <span>New chat</span>
                  </SidebarMenuButton>
                </SidebarMenuItem>
                <SidebarMenuItem>
                  <SidebarMenuButton onClick={() => setCommandOpen(true)} tooltip="Search chats">
                    <IconSearch />
                    <span>Search chats</span>
                    <Kbd className="ml-auto group-data-[collapsible=icon]:hidden">⌘K</Kbd>
                  </SidebarMenuButton>
                </SidebarMenuItem>
              </SidebarMenu>
            </SidebarGroupContent>
          </SidebarGroup>

          {recentConversations.length > 0 ? (
            <SidebarGroup className="group-data-[collapsible=icon]:hidden">
              <SidebarGroupLabel>Recent</SidebarGroupLabel>
              <SidebarGroupContent>
                <SidebarMenu>
                  {recentConversations.map((conversation) => (
                    <ConversationItem
                      conversation={conversation}
                      isActive={routeConversationId === conversation.id}
                      key={conversation.id}
                    />
                  ))}
                  {hasMore ? (
                    <SidebarMenuItem>
                      <SidebarMenuButton
                        className="text-muted-foreground"
                        onClick={() => setCommandOpen(true)}
                      >
                        <IconDots />
                        <span>More</span>
                      </SidebarMenuButton>
                    </SidebarMenuItem>
                  ) : null}
                </SidebarMenu>
              </SidebarGroupContent>
            </SidebarGroup>
          ) : null}
        </SidebarContent>

        <SidebarFooter>
          <NavUser
            user={auth.user}
            onSignOut={() => void signOut()}
            signingOut={signOutResult.waiting}
          />
        </SidebarFooter>
        <SidebarRail />
      </Sidebar>

      <SidebarInset className="min-h-0 min-w-0 overflow-hidden">
        <ConversationView.View
          chat={chat}
          displayName={auth.user.name}
          expectedConversationId={routeConversationId}
          title={activeConversation?.title}
        />
        <Outlet />
      </SidebarInset>

      <ConversationCommandMenu
        open={commandOpen}
        onOpenChange={setCommandOpen}
        conversations={sortedConversations}
        onNewConversation={startNewConversation}
      />
    </SidebarProvider>
  );
}

function conversationIdFromPathname(pathname: string): string | undefined {
  const prefix = "/app/conversations/";
  if (!pathname.startsWith(prefix)) return undefined;

  const segment = pathname.slice(prefix.length).split("/", 1)[0];
  if (segment === undefined || segment.length === 0) return undefined;

  try {
    return decodeURIComponent(segment);
  } catch {
    return segment;
  }
}

function ConversationItem({
  conversation,
  isActive,
}: {
  readonly conversation: ConversationSummary;
  readonly isActive: boolean;
}) {
  return (
    <SidebarMenuItem>
      <SidebarMenuButton
        isActive={isActive}
        render={
          <Link
            params={{ conversationId: conversation.id }}
            to="/app/conversations/$conversationId"
          />
        }
      >
        <span>{conversation.title ?? "Untitled conversation"}</span>
      </SidebarMenuButton>
    </SidebarMenuItem>
  );
}
