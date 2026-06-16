import type * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import { ServerConfig } from "../../src/config/ServerConfig.ts";

export const testAuth: ServerConfig.Auth = {
  secret: Redacted.make("test-better-auth-secret-value-please-change-0001"),
  baseURL: "http://localhost:3000",
  webOrigins: ["http://localhost:3000", "https://app.denora.me"],
  google: {
    clientId: "test-google-client-id.apps.googleusercontent.com",
    clientSecret: Redacted.make("test-google-client-secret"),
  },
};

export const layer = (auth: ServerConfig.Auth = testAuth): Layer.Layer<ServerConfig.Service> =>
  ServerConfig.layer({ auth });
