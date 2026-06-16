import { betterAuth, type BetterAuthOptions } from "better-auth";
import * as Redacted from "effect/Redacted";

/**
 * Shared Better Auth construction.
 *
 * Kept free of any adapter/generated-schema import so the runtime worker bundle
 * doesn't pull in codegen-only dependencies. The runtime layer (`Live.ts`)
 * passes the custom effect-postgres `database` adapter; the CLI entrypoint
 * (`auth.ts`) passes the official drizzle adapter purely to emit the schema.
 */

export const authBasePath = "/api/auth";

export type BetterAuthRuntimeOptions = {
  readonly database: BetterAuthOptions["database"];
  readonly secret: Redacted.Redacted<string>;
  readonly baseURL: string;
  readonly trustedOrigins: ReadonlyArray<string>;
  readonly google: {
    readonly clientId: string;
    readonly clientSecret: Redacted.Redacted<string>;
  };
};

const baseOptions = (options: BetterAuthRuntimeOptions) =>
  ({
    database: options.database,
    secret: Redacted.value(options.secret),
    baseURL: options.baseURL,
    basePath: authBasePath,
    trustedOrigins: [...options.trustedOrigins],
    socialProviders: {
      google: {
        clientId: options.google.clientId,
        clientSecret: Redacted.value(options.google.clientSecret),
      },
    },
    advanced: {
      database: {
        generateId: false,
      },
    },
    account: {
      storeStateStrategy: "cookie",
    },
    plugins: [],
  }) satisfies BetterAuthOptions;

export const makeBetterAuth = (options: BetterAuthRuntimeOptions) =>
  betterAuth(baseOptions(options));

export type DenoraBetterAuth = ReturnType<typeof makeBetterAuth>;

export * as BetterAuth from "./BetterAuth.ts";
