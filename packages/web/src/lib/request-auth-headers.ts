import { createIsomorphicFn } from "@tanstack/react-start";

import { getServerAuthHeaders } from "./server-headers";

type ServerAuthHeadersGetter = () => Headers;

const getRequestAuthHeaders = createIsomorphicFn()
  .client(() => new Headers())
  .server(getServerAuthHeaders);

export async function withAuthForwardingHeaders(
  headers?: HeadersInit,
  options?: {
    readonly getServerAuthHeaders?: ServerAuthHeadersGetter;
  },
) {
  const mergedHeaders = new Headers(headers);
  const authHeaders = options?.getServerAuthHeaders?.() ?? getRequestAuthHeaders();

  authHeaders.forEach((value, key) => {
    if (!mergedHeaders.has(key)) {
      mergedHeaders.set(key, value);
    }
  });

  return mergedHeaders;
}
