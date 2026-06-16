import { createFileRoute, redirect } from "@tanstack/react-router";

import { getServerSession } from "../lib/auth-server.ts";

export const Route = createFileRoute("/")({
  beforeLoad: async ({ context }) => {
    const session = context.auth ?? (await getServerSession());

    throw redirect({
      to: session?.session ? "/app" : "/login",
    });
  },
});
