import { createFileRoute, useRouter } from "@tanstack/react-router";
import { useMutation } from "@tanstack/react-query";

import { getAuthClient } from "../../auth-client.ts";

export const Route = createFileRoute("/_authenticated/app")({
  component: AppHome,
});

function AppHome() {
  const router = useRouter();
  const { auth } = Route.useRouteContext();
  const user = auth.user;

  const signOutMutation = useMutation({
    mutationFn: async () => {
      const result = await getAuthClient().signOut();

      if (result.error) {
        throw new Error(result.error.message || "Unable to sign out.");
      }
    },
    onSuccess: async () => {
      await router.invalidate();
      await router.navigate({ to: "/login" });
    },
  });

  return (
    <main className="auth-page-shell">
      <section className="auth-card">
        <p className="eyebrow">Denora</p>
        <h1>Your agent is ready.</h1>
        <p>
          Signed in as <strong>{user.email}</strong>. This protected route is loaded through the
          same session cookie the API client forwards to the server.
        </p>
        <button
          disabled={signOutMutation.isPending}
          type="button"
          onClick={() => signOutMutation.mutate()}
        >
          {signOutMutation.isPending ? "Signing out..." : "Sign out"}
        </button>
        {signOutMutation.error instanceof Error ? (
          <p className="form-error">{signOutMutation.error.message}</p>
        ) : null}
      </section>
    </main>
  );
}
