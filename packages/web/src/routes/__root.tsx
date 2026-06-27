import type { ReactNode } from "react";
import appCss from "@denora/ui/globals.css?url";
import { TooltipProvider } from "@denora/ui/components/tooltip";
import { RegistryProvider } from "@effect/atom-react";
import { HeadContent, Outlet, Scripts, createRootRouteWithContext } from "@tanstack/react-router";

import { Auth } from "../lib/Auth.ts";
import type { AppRouterContext } from "../router.tsx";

export const Route = createRootRouteWithContext<AppRouterContext>()({
  beforeLoad: async () => ({
    auth: await Auth.getSession(),
  }),
  head: () => ({
    links: [{ rel: "stylesheet", href: appCss }],
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
  return (
    <RegistryProvider>
      <TooltipProvider>
        <Outlet />
      </TooltipProvider>
    </RegistryProvider>
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
