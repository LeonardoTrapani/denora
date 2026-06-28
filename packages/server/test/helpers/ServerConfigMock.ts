import type * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import { ServerConfig } from "../../src/config/ServerConfig.ts";

export const testAuth: ServerConfig.Auth = {
  apiKey: Redacted.make("sk_test_workos_api_key"),
  baseURL: "http://localhost:1338",
  clientId: "client_test_workos_client_id",
  cookieDomain: undefined,
  cookiePassword: Redacted.make("test-workos-cookie-password-value-please-change-0001"),
  webOrigins: ["http://localhost:1337", "https://app.denora.me"],
};

export const testModel: ServerConfig.Model = {
  openRouterApiKey: Redacted.make("sk-or-test-openrouter-api-key"),
};

export const layer = (
  auth: ServerConfig.Auth = testAuth,
  model: ServerConfig.Model = testModel,
): Layer.Layer<ServerConfig.Service> => ServerConfig.layer({ auth, model });
