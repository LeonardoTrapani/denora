import { createServerFn } from "@tanstack/react-start";

import { getAuthClient } from "../auth-client.ts";

type AuthClient = ReturnType<typeof getAuthClient>;
type GetSessionResult = Awaited<ReturnType<AuthClient["getSession"]>>;

export type DenoraAuthSession = NonNullable<GetSessionResult["data"]>;

export const getServerSession = createServerFn({ method: "GET" }).handler(
  async (): Promise<DenoraAuthSession | null> => {
    const result = await getAuthClient().getSession();

    if (result.error) {
      const status = result.error.status;

      if (status === 401) {
        return null;
      }

      throw new Error(`Failed to load session from server${status ? `: ${status}` : ""}`);
    }

    return result.data?.session ? result.data : null;
  },
);
