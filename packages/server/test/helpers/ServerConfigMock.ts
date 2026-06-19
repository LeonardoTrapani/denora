import type * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import { ServerConfig } from "../../src/config/ServerConfig.ts";

export const testAuth: ServerConfig.Auth = {
  apiKey: Redacted.make("sk_test_workos_api_key"),
  baseURL: "http://localhost:3000",
  clientId: "client_test_workos_client_id",
  cookiePassword: Redacted.make("test-workos-cookie-password-value-please-change-0001"),
  e2eAuthSecret: undefined,
  webOrigins: ["http://localhost:3000", "https://app.denora.me"],
};

export const layer = (auth: ServerConfig.Auth = testAuth): Layer.Layer<ServerConfig.Service> =>
  ServerConfig.layer({ auth });
