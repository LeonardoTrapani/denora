import { createFileRoute } from "@tanstack/react-router";
import { WebConfig } from "~/lib/WebConfig.ts";

export const Route = createFileRoute("/")({
  component: Home,
});

function Home() {
  const returnTo = typeof window === "undefined" ? "/" : window.location.href;
  const loginHref = `${WebConfig.apiUrl}/auth/login?returnTo=${encodeURIComponent(returnTo)}`;

  return (
    <main>
      <a href={loginHref}>Sign in</a>
      <form action={`${WebConfig.apiUrl}/auth/logout`} method="post">
        <button type="submit">Sign out</button>
      </form>
    </main>
  );
}
