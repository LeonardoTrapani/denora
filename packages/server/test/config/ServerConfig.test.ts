import { assert, describe, it } from "@effect/vitest";
import * as Config from "effect/Config";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Effect from "effect/Effect";
import * as Redacted from "effect/Redacted";
import { ServerConfig } from "../../src/config/ServerConfig.ts";

const baseEnv: Record<string, string> = {
  WORKOS_API_KEY: "sk_test_workos_api_key",
  WORKOS_CLIENT_ID: "client_test_workos_client_id",
  WORKOS_COOKIE_PASSWORD: "test-workos-cookie-password-value-please-change-0001",
};

const providerFor = (env: Record<string, string>): ConfigProvider.ConfigProvider =>
  ConfigProvider.fromEnv({ env });

const load = (
  env: Record<string, string>,
): Effect.Effect<ServerConfig.Values, Config.ConfigError> =>
  ServerConfig.load.parse(providerFor(env));

describe("ServerConfig.load", () => {
  it.effect("maps a full valid env into the values record", () =>
    Effect.gen(function* () {
      const { auth } = yield* load(baseEnv);
      assert.strictEqual(Redacted.value(auth.apiKey), baseEnv.WORKOS_API_KEY);
      assert.strictEqual(auth.baseURL, ServerConfig.DefaultApiOrigin);
      assert.strictEqual(auth.clientId, baseEnv.WORKOS_CLIENT_ID);
      assert.strictEqual(auth.cookieDomain, undefined);
      assert.strictEqual(Redacted.value(auth.cookiePassword), baseEnv.WORKOS_COOKIE_PASSWORD);
    }),
  );

  describe("baseURL (WORKOS_REDIRECT_BASE_URL)", () => {
    it.effect("defaults to the local API origin when unset", () =>
      Effect.gen(function* () {
        const { auth } = yield* load(baseEnv);
        assert.strictEqual(auth.baseURL, ServerConfig.DefaultApiOrigin);
      }),
    );

    it.effect("is normalized to an origin (path + trailing slash dropped)", () =>
      Effect.gen(function* () {
        const { auth } = yield* load({
          ...baseEnv,
          WORKOS_REDIRECT_BASE_URL: "https://api.denora.me/base/",
        });
        assert.strictEqual(auth.baseURL, "https://api.denora.me");
      }),
    );
  });

  describe("cookieDomain (DENORA_COOKIE_DOMAIN)", () => {
    it.effect("defaults to undefined when unset or blank", () =>
      Effect.gen(function* () {
        const unset = yield* load(baseEnv);
        const blank = yield* load({ ...baseEnv, DENORA_COOKIE_DOMAIN: "   " });

        assert.strictEqual(unset.auth.cookieDomain, undefined);
        assert.strictEqual(blank.auth.cookieDomain, undefined);
      }),
    );

    it.effect("normalizes a leading dot", () =>
      Effect.gen(function* () {
        const { auth } = yield* load({ ...baseEnv, DENORA_COOKIE_DOMAIN: ".dev.denora.me" });

        assert.strictEqual(auth.cookieDomain, "dev.denora.me");
      }),
    );
  });

  describe("webOrigins (DENORA_WEB_ORIGINS)", () => {
    it.effect("defaults to DefaultWebOrigins when unset", () =>
      Effect.gen(function* () {
        const { auth } = yield* load(baseEnv);
        assert.deepStrictEqual(auth.webOrigins, [...ServerConfig.DefaultWebOrigins]);
        assert.deepStrictEqual(
          [...ServerConfig.DefaultWebOrigins],
          [
            "http://localhost:3000",
            "http://localhost:5173",
            "http://localhost:1337",
            "http://localhost:1338",
            "http://localhost:8081",
            "http://127.0.0.1:3000",
            "http://127.0.0.1:5173",
            "http://127.0.0.1:1337",
            "http://127.0.0.1:1338",
            "http://127.0.0.1:8081",
          ],
        );
      }),
    );

    it.effect("falls back to the defaults when set but blank", () =>
      Effect.gen(function* () {
        const { auth } = yield* load({ ...baseEnv, DENORA_WEB_ORIGINS: "   " });
        assert.deepStrictEqual(auth.webOrigins, [...ServerConfig.DefaultWebOrigins]);
      }),
    );

    it.effect("parses a single value", () =>
      Effect.gen(function* () {
        const { auth } = yield* load({ ...baseEnv, DENORA_WEB_ORIGINS: "https://app.denora.me" });
        assert.deepStrictEqual(auth.webOrigins, ["https://app.denora.me"]);
      }),
    );

    it.effect("parses a comma-separated list, trims each entry, and drops blanks", () =>
      Effect.gen(function* () {
        const { auth } = yield* load({
          ...baseEnv,
          DENORA_WEB_ORIGINS: " http://localhost:3000 , , https://app.denora.me ",
        });
        assert.deepStrictEqual(auth.webOrigins, ["http://localhost:3000", "https://app.denora.me"]);
      }),
    );
  });

  describe("missing required keys produce a ConfigError", () => {
    const required = ["WORKOS_API_KEY", "WORKOS_CLIENT_ID", "WORKOS_COOKIE_PASSWORD"] as const;

    it.effect.each(required)("missing %s fails", (key) =>
      Effect.gen(function* () {
        const env = { ...baseEnv };
        delete env[key];
        const error = yield* Effect.flip(load(env));
        assert.strictEqual(error._tag, "ConfigError");
      }),
    );
  });
});
