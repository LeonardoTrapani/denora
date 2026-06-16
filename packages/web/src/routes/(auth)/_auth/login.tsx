import { useMutation } from "@tanstack/react-query";
import { createFileRoute, redirect } from "@tanstack/react-router";

import { getAuthClient } from "../../../auth-client.ts";
import { getServerSession } from "../../../lib/auth-server.ts";

type LoginSearch = {
  readonly redirect?: string | undefined;
};

const normalizeRedirect = (value: unknown) =>
  typeof value === "string" && value.startsWith("/") && !value.startsWith("//") ? value : undefined;

export const Route = createFileRoute("/(auth)/_auth/login")({
  validateSearch: (search): LoginSearch => ({
    redirect: normalizeRedirect(search.redirect),
  }),
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
    <main className="auth-page-shell">
      <section className="auth-card">
        <p className="eyebrow">Denora</p>
        <h1>Sign in to your agent.</h1>
        <p>Use Google to continue into the chat-first control surface.</p>
        <GoogleLogin />
      </section>
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
    <div className="login-form">
      <button
        className="google-login-button"
        disabled={signInMutation.isPending}
        onClick={() => signInMutation.mutate()}
        type="button"
      >
        <span className="google-mark" aria-hidden="true">
          G
        </span>
        {signInMutation.isPending ? "Opening Google..." : "Continue with Google"}
      </button>
      {signInMutation.error instanceof Error ? (
        <p className="form-error">{signInMutation.error.message}</p>
      ) : null}
    </div>
  );
}
