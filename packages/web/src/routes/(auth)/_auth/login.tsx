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
            Use Google to continue into the chat-first control surface.
          </CardDescription>
        </CardHeader>
        <CardContent>
          <GoogleLogin />
        </CardContent>
      </Card>
    </main>
  );
}

function GoogleLogin() {
  const search = Route.useSearch();

  const signInMutation = useMutation({
    mutationFn: async () => {
      const callbackURL = new URL(search.redirect ?? "/app", window.location.origin).toString();
      const errorCallbackURL = new URL("/login", window.location.origin).toString();

      const result = await getAuthClient().signIn.social({
        provider: "google",
        callbackURL,
        disableRedirect: true,
        errorCallbackURL,
      });

      if (result.error) {
        throw new Error(result.error.message || "Unable to start Google sign-in.");
      }

      if (result.data?.url) {
        window.location.assign(result.data.url);
        return result.data;
      }

      throw new Error("Google sign-in did not return a redirect URL.");
    },
    onError: (error) => {
      console.error("Google sign-in failed", error);
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
        {signInMutation.isPending ? "Opening Google..." : "Continue with Google"}
      </Button>
      {signInMutation.error instanceof Error ? (
        <Alert variant="destructive">
          <AlertDescription>{signInMutation.error.message}</AlertDescription>
        </Alert>
      ) : null}
    </div>
  );
}
