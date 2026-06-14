import { assert, describe, it } from "@effect/vitest";
import * as Config from "effect/Config";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Effect from "effect/Effect";
import * as Redacted from "effect/Redacted";
import { ServerConfig } from "../../src/config/ServerConfig.ts";

const COOKIE_PASSWORD_32 = "0123456789abcdef0123456789abcdef"; // exactly 32 chars

const baseEnv: Record<string, string> = {
  WORKOS_API_KEY: "sk_x",
  WORKOS_CLIENT_ID: "client_123",
  CSRF_SECRET: "super-secret",
  WORKOS_COOKIE_PASSWORD: COOKIE_PASSWORD_32,
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

      assert.strictEqual(Redacted.value(auth.apiKey), "sk_x");
      assert.strictEqual(auth.clientId, "client_123");
      assert.strictEqual(Redacted.value(auth.csrfSecret), "super-secret");
      assert.strictEqual(Redacted.value(auth.cookiePassword), COOKIE_PASSWORD_32);
    }),
  );

  describe("webOrigins (DENORA_WEB_ORIGINS)", () => {
    it.effect("defaults to DefaultWebOrigins when unset", () =>
      Effect.gen(function* () {
        const { auth } = yield* load(baseEnv);
        assert.deepStrictEqual(auth.webOrigins, [...ServerConfig.DefaultWebOrigins]);
        assert.deepStrictEqual(
          [...ServerConfig.DefaultWebOrigins],
          ["http://localhost:3000", "http://localhost:8081"],
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

  describe("appRedirectSchemes (DENORA_APP_REDIRECT_SCHEMES)", () => {
    it.effect("defaults to [DefaultAppRedirectScheme] when unset", () =>
      Effect.gen(function* () {
        const { auth } = yield* load(baseEnv);
        assert.deepStrictEqual(auth.appRedirectSchemes, [ServerConfig.DefaultAppRedirectScheme]);
        assert.strictEqual(ServerConfig.DefaultAppRedirectScheme, "denora");
      }),
    );

    it.effect("parses a comma-separated list and trims each entry", () =>
      Effect.gen(function* () {
        const { auth } = yield* load({
          ...baseEnv,
          DENORA_APP_REDIRECT_SCHEMES: " denora , denora-staging ",
        });
        assert.deepStrictEqual(auth.appRedirectSchemes, ["denora", "denora-staging"]);
      }),
    );
  });

  describe("cookieDomain (DENORA_COOKIE_DOMAIN)", () => {
    it.effect("is undefined when unset", () =>
      Effect.gen(function* () {
        const { auth } = yield* load(baseEnv);
        assert.strictEqual(auth.cookieDomain, undefined);
      }),
    );

    it.effect("is undefined when empty", () =>
      Effect.gen(function* () {
        const { auth } = yield* load({ ...baseEnv, DENORA_COOKIE_DOMAIN: "" });
        assert.strictEqual(auth.cookieDomain, undefined);
      }),
    );

    it.effect("is undefined when whitespace only (trimmed to empty)", () =>
      Effect.gen(function* () {
        const { auth } = yield* load({ ...baseEnv, DENORA_COOKIE_DOMAIN: "   " });
        assert.strictEqual(auth.cookieDomain, undefined);
      }),
    );

    it.effect("is the trimmed value when non-empty", () =>
      Effect.gen(function* () {
        const { auth } = yield* load({ ...baseEnv, DENORA_COOKIE_DOMAIN: "  .denora.me  " });
        assert.strictEqual(auth.cookieDomain, ".denora.me");
      }),
    );
  });

  describe("cookiePassword length constraint (WORKOS_COOKIE_PASSWORD)", () => {
    it.effect("accepts exactly 32 chars", () =>
      Effect.gen(function* () {
        const { auth } = yield* load({ ...baseEnv, WORKOS_COOKIE_PASSWORD: COOKIE_PASSWORD_32 });
        assert.strictEqual(Redacted.value(auth.cookiePassword), COOKIE_PASSWORD_32);
      }),
    );

    it.effect("fails on 31 chars", () =>
      Effect.gen(function* () {
        const error = yield* Effect.flip(
          load({ ...baseEnv, WORKOS_COOKIE_PASSWORD: "a".repeat(31) }),
        );
        assert.strictEqual(error._tag, "ConfigError");
      }),
    );

    it.effect("fails on 33 chars", () =>
      Effect.gen(function* () {
        const error = yield* Effect.flip(
          load({ ...baseEnv, WORKOS_COOKIE_PASSWORD: "a".repeat(33) }),
        );
        assert.strictEqual(error._tag, "ConfigError");
      }),
    );
  });

  describe("missing required keys produce a ConfigError", () => {
    const required = [
      "WORKOS_API_KEY",
      "WORKOS_CLIENT_ID",
      "CSRF_SECRET",
      "WORKOS_COOKIE_PASSWORD",
    ] as const;

    it.effect.each(required)("missing %s fails", (key) =>
      Effect.gen(function* () {
        const env = { ...baseEnv };
        delete env[key];
        const error = yield* Effect.flip(load(env));
        assert.strictEqual(error._tag, "ConfigError");
      }),
    );

    it.effect("empty CSRF_SECRET fails (NonEmptyString)", () =>
      Effect.gen(function* () {
        const error = yield* Effect.flip(load({ ...baseEnv, CSRF_SECRET: "" }));
        assert.strictEqual(error._tag, "ConfigError");
      }),
    );

    it.effect("empty WORKOS_CLIENT_ID fails (nonEmptyString)", () =>
      Effect.gen(function* () {
        const error = yield* Effect.flip(load({ ...baseEnv, WORKOS_CLIENT_ID: "" }));
        assert.strictEqual(error._tag, "ConfigError");
      }),
    );
  });
});
