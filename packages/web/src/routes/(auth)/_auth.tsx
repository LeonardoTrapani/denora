import { Outlet, createFileRoute } from "@tanstack/react-router";

export const Route = createFileRoute("/(auth)/_auth")({
  head: () => ({
    meta: [{ name: "robots", content: "noindex, nofollow" }],
  }),
  component: AuthLayout,
});

function AuthLayout() {
  return <Outlet />;
}
