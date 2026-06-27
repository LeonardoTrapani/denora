import { createFileRoute, redirect } from "@tanstack/react-router";

import { Auth } from "../lib/Auth.ts";

export const Route = createFileRoute("/")({
  beforeLoad: async ({ context }) => {
    const session = context.auth ?? (await Auth.getSession());

    throw redirect({
      to: session?.session ? "/app" : "/login",
    });
  },
});
