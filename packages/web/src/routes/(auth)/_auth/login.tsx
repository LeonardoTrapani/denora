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
import { useAtom } from "@effect/atom-react";
import { createFileRoute, redirect } from "@tanstack/react-router";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult";
import * as Atom from "effect/unstable/reactivity/Atom";
import { z } from "zod";

import { Auth } from "../../../lib/Auth.ts";

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
    const session = context.auth ?? (await Auth.getSession());

    if (session?.session) {
      if (search.redirect) {
        throw redirect({ href: search.redirect });
      }

      throw redirect({ to: "/app" });
    }
  },
  component: LoginPage,
});

const signInAtom = Atom.fn<string>()((redirectTo) =>
  Effect.sync(() => {
    Auth.signIn({
      redirect: new URL(redirectTo, window.location.origin).toString(),
      screenHint: "sign-in",
    });
  }),
);

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
  const [signInResult, signIn] = useAtom(signInAtom, { mode: "promise" });
  const error = resultError(signInResult);

  return (
    <div className="grid gap-3">
      <Button
        disabled={signInResult.waiting}
        onClick={() => void signIn(search.redirect ?? "/app")}
        size="lg"
        type="button"
      >
        {signInResult.waiting ? "Opening sign-in..." : "Continue with WorkOS"}
      </Button>
      {error ? (
        <Alert variant="destructive">
          <AlertDescription>{error.message}</AlertDescription>
        </Alert>
      ) : null}
    </div>
  );
}

function resultError(result: AsyncResult.AsyncResult<unknown, unknown>): Error | undefined {
  if (!AsyncResult.isFailure(result)) return undefined;
  return toError(Cause.squash(result.cause));
}

function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}
