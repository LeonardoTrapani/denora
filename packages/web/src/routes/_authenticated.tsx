import { Outlet, createFileRoute, redirect } from "@tanstack/react-router";

import { Auth } from "../lib/Auth.ts";

export const Route = createFileRoute("/_authenticated")({
  beforeLoad: async ({ context, location }) => {
    const session = context.auth ?? (await Auth.getSession());

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
