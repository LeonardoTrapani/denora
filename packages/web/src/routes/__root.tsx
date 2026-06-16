import type { ReactNode } from "react";
import { QueryClientProvider } from "@tanstack/react-query";
import { HeadContent, Outlet, Scripts, createRootRouteWithContext } from "@tanstack/react-router";

import { getServerSession } from "../lib/auth-server.ts";
import type { AppRouterContext } from "../router.tsx";
import stylesUrl from "../styles.css?url";

export const Route = createRootRouteWithContext<AppRouterContext>()({
  beforeLoad: async () => ({
    auth: await getServerSession(),
  }),
  head: () => ({
    links: [{ rel: "stylesheet", href: stylesUrl }],
    meta: [
      { charSet: "utf-8" },
      { name: "viewport", content: "width=device-width, initial-scale=1" },
      {
        name: "description",
        content:
          "Create a named personal agent with explicit identity, permissions, and audit controls.",
      },
      { title: "Denora" },
    ],
  }),
  component: RootComponent,
  shellComponent: RootDocument,
});

function RootComponent() {
  const { queryClient } = Route.useRouteContext();

  return (
    <QueryClientProvider client={queryClient}>
      <Outlet />
    </QueryClientProvider>
  );
}

function RootDocument({ children }: Readonly<{ children: ReactNode }>) {
  return (
    <html lang="en">
      <head>
        <HeadContent />
      </head>
      <body>
        {children}
        <Scripts />
      </body>
    </html>
  );
}
