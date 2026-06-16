import { assert, describe, it } from "@effect/vitest";
import * as Config from "effect/Config";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Effect from "effect/Effect";
import * as Redacted from "effect/Redacted";
import { ServerConfig } from "../../src/config/ServerConfig.ts";

const baseEnv: Record<string, string> = {
  BETTER_AUTH_SECRET: "test-better-auth-secret-value-please-change-0001",
  BETTER_AUTH_URL: "http://localhost:3000",
  GOOGLE_CLIENT_ID: "test-google-client-id.apps.googleusercontent.com",
  GOOGLE_CLIENT_SECRET: "test-google-client-secret",
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
      assert.strictEqual(Redacted.value(auth.secret), baseEnv.BETTER_AUTH_SECRET);
      assert.strictEqual(auth.baseURL, "http://localhost:3000");
      assert.strictEqual(auth.google.clientId, baseEnv.GOOGLE_CLIENT_ID);
      assert.strictEqual(Redacted.value(auth.google.clientSecret), baseEnv.GOOGLE_CLIENT_SECRET);
    }),
  );

  describe("baseURL (BETTER_AUTH_URL)", () => {
    it.effect("is normalized to an origin (path + trailing slash dropped)", () =>
      Effect.gen(function* () {
        const { auth } = yield* load({
          ...baseEnv,
          BETTER_AUTH_URL: "https://api.denora.me/base/",
        });
        assert.strictEqual(auth.baseURL, "https://api.denora.me");
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
          ["http://localhost:3000", "http://localhost:1338", "http://localhost:8081"],
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
    const required = [
      "BETTER_AUTH_SECRET",
      "BETTER_AUTH_URL",
      "GOOGLE_CLIENT_ID",
      "GOOGLE_CLIENT_SECRET",
    ] as const;

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
