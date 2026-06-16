import { Outlet, createFileRoute, redirect } from "@tanstack/react-router";

import { getServerSession } from "../lib/auth-server.ts";

export const Route = createFileRoute("/_authenticated")({
  beforeLoad: async ({ context, location }) => {
    const session = context.auth ?? (await getServerSession());

    if (!session?.session) {
      throw redirect({
        to: "/login",
        search: location.href === "/" ? {} : { redirect: location.href },
      });
    }

    return {
      auth: session,
    };
  },
  component: AuthenticatedLayout,
});

function AuthenticatedLayout() {
  return <Outlet />;
}
