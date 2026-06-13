import { createFileRoute } from "@tanstack/react-router";
import { useEffect, useState } from "react";
import { WebConfig } from "~/lib/WebConfig.ts";

export const Route = createFileRoute("/")({
  component: Home,
});

function Home() {
  const [csrfToken, setCsrfToken] = useState("");
  const returnTo = typeof window === "undefined" ? "/" : window.location.href;
  const loginHref = `${WebConfig.apiUrl}/auth/login?returnTo=${encodeURIComponent(returnTo)}`;
  // TODO: Render user-facing auth error copy from the `authError` query param.

  useEffect(() => {
    const controller = new AbortController();

    void fetch(`${WebConfig.apiUrl}/auth/csrf-token`, {
      credentials: "include",
      signal: controller.signal,
    })
      .then(async (response) => {
        if (!response.ok) return;
        const payload = (await response.json()) as { csrfToken?: unknown };
        if (typeof payload.csrfToken === "string") setCsrfToken(payload.csrfToken);
      })
      .catch(() => {});

    return () => controller.abort();
  }, []);

  return (
    <main>
      <a href={loginHref}>Sign in</a>
      <form action={`${WebConfig.apiUrl}/auth/logout`} method="post">
        <input name="csrfToken" type="hidden" value={csrfToken} />
        <input name="returnTo" type="hidden" value={returnTo} />
        <button disabled={!csrfToken} type="submit">
          Sign out
        </button>
      </form>
    </main>
  );
}
