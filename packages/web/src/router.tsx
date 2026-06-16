import {
  environmentManager,
  QueryClient,
  type QueryClient as QueryClientType,
} from "@tanstack/react-query";
import { createRouter } from "@tanstack/react-router";

import { getAuthClient } from "./auth-client.ts";
import type { DenoraAuthSession } from "./lib/auth-server.ts";
import { routeTree } from "./routeTree.gen";

export interface AppRouterContext {
  readonly auth: DenoraAuthSession | null;
  readonly authClient: ReturnType<typeof getAuthClient>;
  readonly queryClient: QueryClientType;
}

function createQueryClient() {
  return new QueryClient({
    defaultOptions: {
      queries: {
        staleTime: 30 * 1000,
        gcTime: 5 * 60 * 1000,
        refetchOnWindowFocus: false,
        retry: 1,
        retryOnMount: false,
      },
      mutations: {
        retry: false,
      },
    },
  });
}

let browserQueryClient: QueryClientType | undefined;

function getQueryClient() {
  if (environmentManager.isServer()) return createQueryClient();

  browserQueryClient ??= createQueryClient();
  return browserQueryClient;
}

export function getRouter() {
  const queryClient = getQueryClient();

  return createRouter({
    context: {
      auth: null,
      authClient: getAuthClient(),
      queryClient,
    } satisfies AppRouterContext,
    routeTree,
    defaultPreload: "intent",
    scrollRestoration: true,
  });
}
