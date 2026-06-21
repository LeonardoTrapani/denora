import { Button } from "@denora/ui/components/button";
import { useMutation } from "@tanstack/react-query";
import { createFileRoute } from "@tanstack/react-router";

import { getAuthClient } from "../../auth-client.ts";

export const Route = createFileRoute("/_authenticated/app")({
  component: AppHome,
});

function AppHome() {
  const { auth } = Route.useRouteContext();
  const user = auth.user;
  const signOutMutation = useMutation({
    mutationFn: async () => {
      getAuthClient().signOut();
    },
  });

  return (
    <main>
      {JSON.stringify(user)}
      <Button
        className="mt-3 w-full"
        disabled={signOutMutation.isPending}
        size="sm"
        type="button"
        variant="outline"
        onClick={() => signOutMutation.mutate()}
      >
        {signOutMutation.isPending ? "Signing out..." : "Sign out"}
      </Button>
      ;
    </main>
  );
}
