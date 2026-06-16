import { createAuthClient } from "better-auth/client";

import { withAuthForwardingHeaders } from "./lib/request-auth-headers";
import { WebConfig } from "./lib/WebConfig.ts";

const makeAuthClient = (baseURL: string) =>
  createAuthClient({
    baseURL,
    basePath: "/api/auth",
    fetchOptions: {
      credentials: "include",
      async onRequest(context) {
        return {
          ...context,
          headers: await withAuthForwardingHeaders(context.headers),
        };
      },
    },
  });

export const getAuthClient = () => makeAuthClient(WebConfig.requireApiUrl());
