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
              <AccountSkeleton />
            </SidebarMenuItem>
          </SidebarMenu>
        </SidebarHeader>
        <SidebarContent>
          <SidebarGroup>
            <SidebarGroupContent>
              <SidebarMenu>
                <SidebarMenuItem>
                  <SidebarMenuSkeleton showIcon />
                </SidebarMenuItem>
                <SidebarMenuItem>
                  <SidebarMenuSkeleton showIcon />
                </SidebarMenuItem>
              </SidebarMenu>
            </SidebarGroupContent>
          </SidebarGroup>
          <SidebarGroup>
            <SidebarGroupLabel>Recent</SidebarGroupLabel>
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
              <AccountSkeleton />
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

function AccountSkeleton() {
  return (
    <div className="flex items-center gap-2 p-2">
      <Skeleton className="size-8 rounded-lg" />
      <div className="grid flex-1 gap-1.5">
        <Skeleton className="h-3.5 w-24" />
        <Skeleton className="h-3 w-32" />
      </div>
    </div>
  );
}

export function ConversationPanelSkeleton() {
  return (
    <div className="flex h-svh min-h-0 flex-col overflow-hidden">
      <header className="flex h-14 shrink-0 items-center gap-2 border-b px-4">
        <SidebarTrigger />
        <Skeleton className="h-4 w-28" />
      </header>

      <section className="flex min-h-0 flex-1 flex-col overflow-hidden">
        <div className="mx-auto flex min-h-0 w-full max-w-3xl flex-1 flex-col justify-end gap-8 overflow-hidden px-4 py-6">
          <ConversationHistorySkeleton />
        </div>
        <ComposerSkeleton />
      </section>
    </div>
  );
}

export function ConversationHistorySkeleton() {
  return (
    <div aria-hidden="true" className="flex flex-col gap-8">
      <UserMessageSkeleton width="w-48" />
      <AssistantMessageSkeleton lines={3} />
      <UserMessageSkeleton width="w-32" />
      <AssistantMessageSkeleton lines={2} />
    </div>
  );
}

function AssistantMessageSkeleton({ lines }: { readonly lines: number }) {
  return (
    <div className="flex flex-col gap-2.5">
      {Array.from({ length: lines }, (_, index) => (
        <Skeleton className={`h-4 ${index === lines - 1 ? "w-2/3" : "w-full"}`} key={index} />
      ))}
    </div>
  );
}

function UserMessageSkeleton({ width }: { readonly width: string }) {
  return (
    <div className="flex justify-end">
      <Skeleton className={`h-9 rounded-3xl ${width}`} />
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
    <div className="shrink-0 px-4 pb-4">
      <div className="mx-auto w-full max-w-3xl">
        <Skeleton className="h-20 w-full rounded-2xl" />
      </div>
    </div>
  );
}

export * as LoadingStates from "./LoadingStates.tsx";
