import type * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import { ServerConfig } from "../../src/config/ServerConfig.ts";

// WorkOS requires the cookie password to be exactly 32 bytes; keep the fixture
// honest so anything exercising the real seal/unseal path stays valid.
export const testAuth: ServerConfig.Auth = {
  apiKey: Redacted.make("sk_test_workos_api_key"),
  clientId: "client_test",
  csrfSecret: Redacted.make("test-csrf-secret-value"),
  cookiePassword: Redacted.make("0123456789abcdef0123456789abcdef"),
  cookieDomain: undefined,
  appRedirectSchemes: ["denora"],
  webOrigins: ["http://localhost:3000", "https://app.denora.me"],
};

export const layer = (auth: ServerConfig.Auth = testAuth): Layer.Layer<ServerConfig.Service> =>
  ServerConfig.layer({ auth });
