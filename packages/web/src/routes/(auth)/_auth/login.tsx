import { Alert, AlertDescription } from "@denora/ui/components/alert";
import { Badge } from "@denora/ui/components/badge";
import { Button } from "@denora/ui/components/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@denora/ui/components/card";
import { useMutation } from "@tanstack/react-query";
import { createFileRoute, redirect } from "@tanstack/react-router";
import { z } from "zod";

import { getAuthClient } from "../../../auth-client.ts";
import { getServerSession } from "../../../lib/auth-server.ts";

const loginSearchSchema = z
  .object({
    redirect: z
      .string()
      .refine((value) => value.startsWith("/") && !value.startsWith("//"))
      .optional()
      .catch(undefined),
  })
  .catch({ redirect: undefined });

type LoginSearch = z.infer<typeof loginSearchSchema>;

export const Route = createFileRoute("/(auth)/_auth/login")({
  validateSearch: (search): LoginSearch => loginSearchSchema.parse(search),
  beforeLoad: async ({ context, search }) => {
    const session = context.auth ?? (await getServerSession());

    if (session?.session) {
      if (search.redirect) {
        throw redirect({ href: search.redirect });
      }

      throw redirect({ to: "/app" });
    }
  },
  component: LoginPage,
});

function LoginPage() {
  return (
    <main className="grid min-h-svh place-items-center p-4">
      <Card className="w-full max-w-md" size="sm">
        <CardHeader>
          <Badge variant="secondary">Denora</Badge>
          <CardTitle>Sign in to your agent.</CardTitle>
          <CardDescription>
            Continue with WorkOS AuthKit to enter the chat-first control surface.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <AuthKitLogin />
        </CardContent>
      </Card>
    </main>
  );
}

function AuthKitLogin() {
  const search = Route.useSearch();

  const signInMutation = useMutation({
    mutationFn: async () => {
      getAuthClient().signIn({
        redirect: new URL(search.redirect ?? "/app", window.location.origin).toString(),
        screenHint: "sign-in",
      });
    },
    onError: (error) => {
      console.error("AuthKit sign-in failed", error);
    },
  });

  return (
    <div className="grid gap-3">
      <Button
        disabled={signInMutation.isPending}
        onClick={() => signInMutation.mutate()}
        size="lg"
        type="button"
      >
        {signInMutation.isPending ? "Opening sign-in..." : "Continue with WorkOS"}
      </Button>
      {signInMutation.error instanceof Error ? (
        <Alert variant="destructive">
          <AlertDescription>{signInMutation.error.message}</AlertDescription>
        </Alert>
      ) : null}
    </div>
  );
}
