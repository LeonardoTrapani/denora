import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { Auth } from "../../src/auth/Auth.ts";
import { AuthLive } from "../../src/auth/Live.ts";
import * as ServerConfigMock from "../helpers/ServerConfigMock.ts";

const authLayer = AuthLive.layer(ServerConfigMock.testAuth);
const prodAuth = {
  ...ServerConfigMock.testAuth,
  baseURL: "https://api.denora.me",
  cookieDomain: "denora.me",
  webOrigins: ["https://denora.me"],
};

const runAuth = (request: Request) =>
  Effect.gen(function* () {
    const auth = yield* Auth.Service;
    return yield* auth.handle(request);
  }).pipe(Effect.provide(authLayer));

const runProdAuth = (request: Request) =>
  Effect.gen(function* () {
    const auth = yield* Auth.Service;
    return yield* auth.handle(request);
  }).pipe(Effect.provide(AuthLive.layer(prodAuth)));

describe("AuthLive", () => {
  it.effect("starts WorkOS AuthKit login with PKCE and a callback-scoped transaction cookie", () =>
    Effect.gen(function* () {
      const response = yield* runAuth(
        new Request("http://localhost:1338/api/auth/login?redirect=/app&screen_hint=sign-up"),
      );

      assert.strictEqual(response.status, 302);

      const location = response.headers.get("location");
      assert.ok(location);

      const url = new URL(location);
      assert.strictEqual(url.origin, "https://api.workos.com");
      assert.strictEqual(url.pathname, "/user_management/authorize");
      assert.strictEqual(url.searchParams.get("client_id"), ServerConfigMock.testAuth.clientId);
      assert.strictEqual(url.searchParams.get("provider"), "authkit");
      assert.strictEqual(url.searchParams.get("response_type"), "code");
      assert.strictEqual(url.searchParams.get("code_challenge_method"), "S256");
      assert.strictEqual(url.searchParams.get("screen_hint"), "sign-up");
      assert.strictEqual(
        url.searchParams.get("redirect_uri"),
        "http://localhost:1338/api/auth/callback",
      );

      const setCookie = response.headers.get("set-cookie");
      assert.ok(setCookie);
      assert.match(setCookie, /^denora_auth_transaction=/);
      assert.match(setCookie, /Path=\/api\/auth\/callback/);
      assert.match(setCookie, /HttpOnly/);
      assert.match(setCookie, /SameSite=Lax/);
      assert.match(setCookie, /Max-Age=600/);
      assert.ok(!/Secure/.test(setCookie));
    }),
  );

  it.effect("keeps the local auth callback on the configured API origin", () =>
    Effect.gen(function* () {
      const response = yield* runAuth(
        new Request(
          "http://127.0.0.1:40773/api/auth/login?redirect=http%3A%2F%2Flocalhost%3A1337%2Fapp",
        ),
      );

      assert.strictEqual(response.status, 302);

      const location = response.headers.get("location");
      assert.ok(location);
      assert.strictEqual(
        new URL(location).searchParams.get("redirect_uri"),
        "http://localhost:1338/api/auth/callback",
      );
    }),
  );

  it.effect("sets prod auth cookies for the shared web/API domain", () =>
    Effect.gen(function* () {
      const response = yield* runProdAuth(
        new Request("https://api.denora.me/api/auth/login?redirect=https%3A%2F%2Fdenora.me%2Fapp"),
      );

      assert.strictEqual(response.status, 302);

      const location = response.headers.get("location");
      assert.ok(location);
      assert.strictEqual(
        new URL(location).searchParams.get("redirect_uri"),
        "https://api.denora.me/api/auth/callback",
      );

      const setCookie = response.headers.get("set-cookie");
      assert.ok(setCookie);
      assert.match(setCookie, /^denora_auth_transaction=/);
      assert.match(setCookie, /Secure/);
      assert.match(setCookie, /Domain=denora\.me/);
    }),
  );

  it.effect("clears an invalid session cookie when session lookup is unauthenticated", () =>
    Effect.gen(function* () {
      const response = yield* runAuth(
        new Request("http://localhost:1338/api/auth/session", {
          headers: { cookie: "denora_session=bogus" },
        }),
      );

      assert.strictEqual(response.status, 401);
      assert.deepStrictEqual(yield* Effect.promise(() => response.json()), {
        session: null,
        user: null,
      });

      const setCookie = response.headers.get("set-cookie");
      assert.ok(setCookie);
      assert.match(setCookie, /^denora_session=/);
      assert.match(setCookie, /Max-Age=0/);
      assert.match(setCookie, /Expires=Thu, 01 Jan 1970 00:00:00 GMT/);
      assert.match(setCookie, /HttpOnly/);
      assert.match(setCookie, /SameSite=Lax/);
    }),
  );

  it.effect("logs out locally when the sealed session cannot yield a WorkOS session id", () =>
    Effect.gen(function* () {
      const response = yield* runAuth(
        new Request("http://localhost:1338/api/auth/logout?return_to=/login", {
          headers: { cookie: "denora_session=bogus" },
        }),
      );

      assert.strictEqual(response.status, 302);
      assert.strictEqual(response.headers.get("location"), "http://localhost:1337/login");

      const setCookie = response.headers.get("set-cookie");
      assert.ok(setCookie);
      assert.match(setCookie, /denora_session=/);
      assert.match(setCookie, /denora_auth_transaction=/);
      assert.match(setCookie, /Max-Age=0/);
      assert.match(setCookie, /Expires=Thu, 01 Jan 1970 00:00:00 GMT/);
    }),
  );
});
