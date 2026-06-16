import { drizzleAdapter } from "better-auth/adapters/drizzle";
import * as Redacted from "effect/Redacted";
import { makeBetterAuth } from "./src/auth/BetterAuth.ts";

// The Better Auth CLI reads this instance's plugin/field shape to emit the
// Drizzle schema (`bun run auth:schema`). It only generates text — the drizzle
// adapter here never opens a connection. Runtime auth uses the custom
// effect-postgres adapter wired in src/auth/Live.ts, not this one.
export const auth = makeBetterAuth({
  database: drizzleAdapter({} as never, { provider: "pg" }),
  secret: Redacted.make("denora-better-auth-codegen-secret-not-used-at-runtime-00"),
  baseURL: "http://localhost:3000",
  trustedOrigins: [],
  google: {
    clientId: "denora-better-auth-codegen-google-client-id",
    clientSecret: Redacted.make("denora-better-auth-codegen-google-client-secret"),
  },
});

export default auth;
