import { Skeleton } from "@denora/ui/components/skeleton";
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
  SidebarMenuItem,
  SidebarMenuSkeleton,
  SidebarProvider,
  SidebarTrigger,
} from "@denora/ui/components/sidebar";

export function FullPageSkeleton() {
  return (
    <main className="grid min-h-svh place-items-center p-4">
      <div className="grid w-full max-w-md gap-4 rounded-2xl border p-6">
        <Skeleton className="h-5 w-20" />
        <div className="grid gap-2">
          <Skeleton className="h-7 w-3/4" />
          <Skeleton className="h-4 w-full" />
          <Skeleton className="h-4 w-5/6" />
        </div>
        <Skeleton className="h-10 w-full" />
      </div>
    </main>
  );
}

export function AppShellSkeleton() {
  return (
    <SidebarProvider>
      <Sidebar collapsible="offcanvas">
        <SidebarHeader>
          <SidebarMenu>
            <SidebarMenuItem>
              <Skeleton className="h-8 w-24" />
            </SidebarMenuItem>
          </SidebarMenu>
          <Skeleton className="h-9 w-full" />
        </SidebarHeader>
        <SidebarContent>
          <SidebarGroup>
            <SidebarGroupLabel>Conversations</SidebarGroupLabel>
            <SidebarGroupContent>
              <SidebarMenu>
                {Array.from({ length: 6 }, (_, index) => (
                  <SidebarMenuItem key={index}>
                    <SidebarMenuSkeleton />
                  </SidebarMenuItem>
                ))}
              </SidebarMenu>
            </SidebarGroupContent>
          </SidebarGroup>
        </SidebarContent>
        <SidebarFooter>
          <SidebarMenu>
            <SidebarMenuItem>
              <SidebarMenuSkeleton />
            </SidebarMenuItem>
            <SidebarMenuItem>
              <SidebarMenuSkeleton />
            </SidebarMenuItem>
          </SidebarMenu>
        </SidebarFooter>
      </Sidebar>
      <SidebarInset className="min-h-0 min-w-0 overflow-hidden">
        <ConversationPanelSkeleton />
      </SidebarInset>
    </SidebarProvider>
  );
}

export function ConversationPanelSkeleton() {
  return (
    <div className="flex h-svh min-h-0 flex-col overflow-hidden">
      <header className="flex h-14 shrink-0 items-center justify-between border-b px-4">
        <div className="flex items-center gap-2">
          <SidebarTrigger />
          <Skeleton className="h-5 w-28" />
        </div>
        <Skeleton className="h-7 w-24" />
      </header>

      <section className="flex min-h-0 flex-1 flex-col overflow-hidden">
        <div className="mx-auto flex min-h-0 w-full max-w-3xl flex-1 flex-col justify-end gap-5 overflow-hidden p-4">
          <ConversationHistorySkeleton />
        </div>
        <ComposerSkeleton />
      </section>
    </div>
  );
}

export function ConversationHistorySkeleton() {
  return (
    <div aria-hidden="true" className="flex flex-col gap-5">
      <AssistantMessageSkeleton lines={2} width="w-4/5" />
      <UserMessageSkeleton width="w-2/3" />
      <AssistantMessageSkeleton lines={3} width="w-5/6" />
      <UserMessageSkeleton width="w-1/2" />
    </div>
  );
}

function AssistantMessageSkeleton({
  lines,
  width,
}: {
  readonly lines: number;
  readonly width: string;
}) {
  return (
    <div className="flex max-w-3xl items-start gap-3">
      <Skeleton className="size-8 shrink-0 rounded-full" />
      <div className="grid flex-1 gap-2">
        <Skeleton className="h-3 w-16" />
        <div className="grid gap-2 rounded-2xl bg-muted/40 p-3">
          {Array.from({ length: lines }, (_, index) => (
            <Skeleton className={`h-4 ${index === lines - 1 ? width : "w-full"}`} key={index} />
          ))}
        </div>
      </div>
    </div>
  );
}

function UserMessageSkeleton({ width }: { readonly width: string }) {
  return (
    <div className="flex justify-end">
      <div className="grid max-w-2xl gap-2">
        <Skeleton className="ml-auto h-3 w-10" />
        <div className="rounded-2xl bg-muted/40 p-3">
          <Skeleton className={`h-4 ${width}`} />
        </div>
      </div>
    </div>
  );
}

export function AssistantTyping() {
  return (
    <div aria-label="Denora is thinking" className="flex items-center gap-1 py-1">
      <Skeleton className="size-2 rounded-full" />
      <Skeleton className="size-2 rounded-full" />
      <Skeleton className="size-2 rounded-full" />
    </div>
  );
}

function ComposerSkeleton() {
  return (
    <div className="shrink-0 border-t bg-background p-4">
      <div className="mx-auto flex w-full max-w-3xl items-end gap-2">
        <Skeleton className="h-16 flex-1" />
        <Skeleton className="h-9 w-16" />
      </div>
    </div>
  );
}

export * as LoadingStates from "./LoadingStates.tsx";
