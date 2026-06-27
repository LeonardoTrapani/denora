import { createRouter } from "@tanstack/react-router";

import { LoadingStates } from "./chat/LoadingStates.tsx";
import type { DenoraAuthSession } from "./lib/Auth.ts";
import { routeTree } from "./routeTree.gen";

export interface AppRouterContext {
  readonly auth: DenoraAuthSession | null;
}

export function getRouter() {
  return createRouter({
    context: {
      auth: null,
    } satisfies AppRouterContext,
    routeTree,
    defaultPreload: "intent",
    defaultPendingComponent: LoadingStates.FullPageSkeleton,
    defaultPendingMs: 100,
    defaultPendingMinMs: 200,
    scrollRestoration: true,
  });
}
