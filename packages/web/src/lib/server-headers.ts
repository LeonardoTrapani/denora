import { createServerOnlyFn } from "@tanstack/react-start";
import { getRequestHeader } from "@tanstack/react-start/server";

const forwardedHeaderNames = ["authorization", "cookie", "x-denora-dev-user-id"] as const;

export const getServerAuthHeaders = createServerOnlyFn(() => {
  const headers = new Headers();

  for (const name of forwardedHeaderNames) {
    const value = getRequestHeader(name);
    if (value) {
      headers.set(name, value);
    }
  }

  return headers;
});
