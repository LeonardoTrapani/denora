import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Cookies from "effect/unstable/http/Cookies";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import * as HttpClient from "effect/unstable/http/HttpClient";
import { AuthRoutes } from "../../src/http/auth/Routes.ts";
import { WorkOsAuth } from "../../src/auth/WorkOsAuth.ts";
import { makeDenoraUser } from "../helpers/fixtures.ts";
import * as ServerConfigMock from "../helpers/ServerConfigMock.ts";
import * as TestServer from "../helpers/TestServer.ts";
import * as WorkOsAuthMock from "../helpers/WorkOsAuthMock.ts";

const SessionCookieName = "wos-session";
const AllowedOrigin = "https://app.denora.me";
const FallbackOrigin = "http://localhost:3000";

// The test HttpClient is fetch-backed and fetch follows 3xx redirects by
// default, which would chase Location into the public internet (DNS failures
// for app.denora.me / workos). `redirect: "manual"` keeps the 3xx response so
// we can assert status + Location ourselves. RequestInit is read from the
// fiber context at request time, so merging it into the served layer applies.
const noFollowRedirects = Layer.succeed(FetchHttpClient.RequestInit, { redirect: "manual" });

// Serves only the /auth/* routes, with the WorkOS service and ServerConfig
// mocked. Each test supplies the WorkOS stubs it needs; unstubbed methods throw
// loudly via WorkOsAuthMock.
const serve = (overrides: Partial<WorkOsAuth.Interface>) =>
  TestServer.layer(
    AuthRoutes.routes.pipe(
      Layer.provide(WorkOsAuthMock.layer(overrides)),
      Layer.provide(ServerConfigMock.layer()),
    ),
  ).pipe(Layer.provideMerge(noFollowRedirects));

// Captures the input passed to a stubbed method so the test can assert on what
// the route forwarded (e.g. the `state`/`returnTo` derivation).
const makeCapture = <I>() => {
  let captured: I | undefined;
  return {
    record: (input: I) => {
      captured = input;
    },
    get: () => captured,
  };
};

const getSetCookie = (res: { readonly cookies: Cookies.Cookies }, name: string) =>
  Option.getOrUndefined(Cookies.get(res.cookies, name));

describe("AuthRoutes", () => {
  // GET /auth/login derives the callback redirect origin from `request.url`
  // (`new URL("/auth/callback", new URL(request.url).origin)`), so it needs an
  // ABSOLUTE request url. The Cloudflare Workers runtime this deploys to passes
  // an absolute `Request.url`, so login works in production. The Node test
  // server, however, populates `request.url` with the path only (like Node's
  // IncomingMessage.url, "/auth/login?..."), so `URL.canParse` is false and the
  // handler safely degrades to `authError=login_failed` without calling WorkOS.
  // The happy-path cases below are therefore skipped here — unskip them if the
  // handler is changed to derive the origin from the Host header / ServerConfig
  // (which would also make login runtime-agnostic rather than Worker-only).
  describe("GET /auth/login", () => {
    it.effect.skip("allowed returnTo -> 302 to authorization URL with returnTo as state", () => {
      const capture = makeCapture<{ readonly redirectUri: string; readonly returnTo: string }>();
      const authorizationUrl = "https://auth.example/authorize?client_id=abc";
      return Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const res = yield* client.get(
          `/auth/login?returnTo=${encodeURIComponent(`${AllowedOrigin}/x`)}`,
        );

        assert.strictEqual(res.status, 302);
        assert.strictEqual(res.headers.location, authorizationUrl);

        const input = capture.get();
        assert.isDefined(input);
        // returnTo is forwarded as the WorkOS `state`; the allowed origin URL is
        // normalized by `new URL().toString()`.
        assert.strictEqual(input!.returnTo, `${AllowedOrigin}/x`);
        // redirectUri is derived from the request origin, not the returnTo.
        assert.match(input!.redirectUri, /\/auth\/callback$/);
      }).pipe(
        Effect.provide(
          serve({
            getAuthorizationUrl: (input) => {
              capture.record(input);
              return Effect.succeed(authorizationUrl);
            },
          }),
        ),
      );
    });

    it.effect.skip("disallowed returnTo -> state falls back to allowed origin", () => {
      const capture = makeCapture<{ readonly redirectUri: string; readonly returnTo: string }>();
      const authorizationUrl = "https://auth.example/authorize?fallback";
      return Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const res = yield* client.get(
          `/auth/login?returnTo=${encodeURIComponent("https://evil.example/phish")}`,
        );

        assert.strictEqual(res.status, 302);
        // Login still succeeds (302 to the authorization URL).
        assert.strictEqual(res.headers.location, authorizationUrl);

        const input = capture.get();
        assert.isDefined(input);
        // The first allowed web origin is the fallback destination.
        assert.strictEqual(input!.returnTo, FallbackOrigin);
      }).pipe(
        Effect.provide(
          serve({
            getAuthorizationUrl: (input) => {
              capture.record(input);
              return Effect.succeed(authorizationUrl);
            },
          }),
        ),
      );
    });

    // Documents the Node test-server behavior: because `request.url` is relative
    // here, the origin can't be derived and login degrades to login_failed
    // before getAuthorizationUrl is consulted (the stub stays uncalled). On
    // Cloudflare Workers `request.url` is absolute and login proceeds normally.
    it.effect("degrades to authError=login_failed under a relative request.url", () => {
      const capture = makeCapture<{ readonly redirectUri: string; readonly returnTo: string }>();
      return Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const res = yield* client.get(
          `/auth/login?returnTo=${encodeURIComponent(`${AllowedOrigin}/dash`)}`,
        );

        assert.strictEqual(res.status, 302);
        const url = new URL(res.headers.location!);
        assert.strictEqual(url.origin, AllowedOrigin);
        assert.strictEqual(url.searchParams.get("authError"), "login_failed");
        assert.isUndefined(capture.get());
      }).pipe(
        Effect.provide(
          serve({
            getAuthorizationUrl: (input) => {
              capture.record(input);
              return Effect.succeed("https://auth.example/authorize");
            },
          }),
        ),
      );
    });
  });

  describe("GET /auth/callback", () => {
    it.effect("valid code+state -> 302 to state destination + Set-Cookie wos-session", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const res = yield* client.get(
          `/auth/callback?code=abc&state=${encodeURIComponent(`${AllowedOrigin}/welcome`)}`,
        );

        assert.strictEqual(res.status, 302);
        assert.strictEqual(res.headers.location, `${AllowedOrigin}/welcome`);

        const cookie = getSetCookie(res, SessionCookieName);
        assert.isDefined(cookie);
        assert.strictEqual(cookie!.value, "sealed");
      }).pipe(
        Effect.provide(
          serve({
            authenticateWithCode: () =>
              Effect.succeed({ user: makeDenoraUser(), sealedSession: "sealed" }),
          }),
        ),
      ),
    );

    it.effect("forwards the callback code to authenticateWithCode", () => {
      const capture = makeCapture<{ readonly code: string }>();
      return Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const res = yield* client.get(
          `/auth/callback?code=the-code&state=${encodeURIComponent(AllowedOrigin)}`,
        );
        assert.strictEqual(res.status, 302);
        assert.strictEqual(capture.get()?.code, "the-code");
      }).pipe(
        Effect.provide(
          serve({
            authenticateWithCode: (input) => {
              capture.record(input);
              return Effect.succeed({ user: makeDenoraUser(), sealedSession: "sealed" });
            },
          }),
        ),
      );
    });

    it.effect("missing code -> 302 authError=callback_missing_code", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const res = yield* client.get(
          `/auth/callback?state=${encodeURIComponent(`${AllowedOrigin}/welcome`)}`,
        );

        assert.strictEqual(res.status, 302);
        const url = new URL(res.headers.location!);
        assert.strictEqual(url.origin, AllowedOrigin);
        assert.strictEqual(url.searchParams.get("authError"), "callback_missing_code");
        assert.isUndefined(getSetCookie(res, SessionCookieName));
      }).pipe(Effect.provide(serve({}))),
    );

    it.effect("authenticateWithCode WorkOsAuthError -> authError=callback_failed", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const res = yield* client.get(
          `/auth/callback?code=abc&state=${encodeURIComponent(AllowedOrigin)}`,
        );

        assert.strictEqual(res.status, 302);
        const url = new URL(res.headers.location!);
        assert.strictEqual(url.searchParams.get("authError"), "callback_failed");
        assert.isUndefined(getSetCookie(res, SessionCookieName));
      }).pipe(
        Effect.provide(
          serve({
            authenticateWithCode: () =>
              Effect.fail(
                new WorkOsAuth.WorkOsAuthError({
                  operation: "authenticateWithCode",
                  cause: new Error("denied"),
                }),
              ),
          }),
        ),
      ),
    );

    it.effect("authenticateWithCode UserSyncError -> authError=user_sync_failed", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const res = yield* client.get(
          `/auth/callback?code=abc&state=${encodeURIComponent(AllowedOrigin)}`,
        );

        assert.strictEqual(res.status, 302);
        const url = new URL(res.headers.location!);
        assert.strictEqual(url.searchParams.get("authError"), "user_sync_failed");
        assert.isUndefined(getSetCookie(res, SessionCookieName));
      }).pipe(
        Effect.provide(
          serve({
            authenticateWithCode: () =>
              Effect.fail(
                new WorkOsAuth.UserSyncError({
                  workosUserId: "user_01HZX",
                  cause: new Error("db down"),
                }),
              ),
          }),
        ),
      ),
    );
  });

  describe("GET /auth/csrf-token", () => {
    it.effect("200, application/json, no-store, body { csrfToken } that validates", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const res = yield* client.get("/auth/csrf-token");

        assert.strictEqual(res.status, 200);
        assert.match(res.headers["content-type"] ?? "", /application\/json/);
        assert.strictEqual(res.headers["cache-control"], "no-store");

        const body = (yield* res.json) as { readonly csrfToken: string };
        assert.isString(body.csrfToken);
        // No session cookie sent -> token bound to undefined session.
        assert.isTrue(
          AuthRoutes.isValidCsrfToken(ServerConfigMock.testAuth, body.csrfToken, undefined),
        );
        // A token bound to undefined must NOT validate against a real session.
        assert.isFalse(
          AuthRoutes.isValidCsrfToken(ServerConfigMock.testAuth, body.csrfToken, "sess1"),
        );
      }).pipe(Effect.provide(serve({}))),
    );

    it.effect("token is bound to the session cookie it was issued with", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const res = yield* client.get("/auth/csrf-token", {
          headers: { cookie: `${SessionCookieName}=sess1` },
        });

        assert.strictEqual(res.status, 200);
        const body = (yield* res.json) as { readonly csrfToken: string };
        assert.isTrue(
          AuthRoutes.isValidCsrfToken(ServerConfigMock.testAuth, body.csrfToken, "sess1"),
        );
        assert.isFalse(
          AuthRoutes.isValidCsrfToken(ServerConfigMock.testAuth, body.csrfToken, "sess2"),
        );
      }).pipe(Effect.provide(serve({}))),
    );
  });

  describe("POST /auth/logout", () => {
    it.effect("missing CSRF token -> 403 Invalid CSRF token", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const res = yield* client.post("/auth/logout", {
          headers: { cookie: `${SessionCookieName}=sess1` },
        });

        assert.strictEqual(res.status, 403);
        assert.strictEqual(yield* res.text, "Invalid CSRF token");
      }).pipe(Effect.provide(serve({}))),
    );

    it.effect("invalid CSRF token -> 403 Invalid CSRF token", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const res = yield* client.post("/auth/logout", {
          headers: {
            cookie: `${SessionCookieName}=sess1`,
            "x-csrf-token": "not.a.validtoken",
          },
        });

        assert.strictEqual(res.status, 403);
        assert.strictEqual(yield* res.text, "Invalid CSRF token");
      }).pipe(Effect.provide(serve({}))),
    );

    it.effect("valid CSRF -> 302 to logout url + Set-Cookie clearing wos-session", () =>
      Effect.gen(function* () {
        const logoutUrl = "https://workos/logout";
        const client = yield* HttpClient.HttpClient;

        // Obtain a token bound to sess1.
        const tokenRes = yield* client.get("/auth/csrf-token", {
          headers: { cookie: `${SessionCookieName}=sess1` },
        });
        const { csrfToken } = (yield* tokenRes.json) as { readonly csrfToken: string };

        const res = yield* client.post("/auth/logout", {
          headers: {
            cookie: `${SessionCookieName}=sess1`,
            "x-csrf-token": csrfToken,
          },
        });

        assert.strictEqual(res.status, 302);
        assert.strictEqual(res.headers.location, logoutUrl);

        const cleared = getSetCookie(res, SessionCookieName);
        assert.isDefined(cleared);
        // Cleared cookie has an empty value and is expired (epoch / max-age 0).
        assert.strictEqual(cleared!.value, "");
      }).pipe(
        Effect.provide(
          serve({
            getLogoutUrl: () => Effect.succeed("https://workos/logout"),
          }),
        ),
      ),
    );

    it.effect("CSRF token bound to sess1 is rejected when posting with sess2", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;

        const tokenRes = yield* client.get("/auth/csrf-token", {
          headers: { cookie: `${SessionCookieName}=sess1` },
        });
        const { csrfToken } = (yield* tokenRes.json) as { readonly csrfToken: string };

        const res = yield* client.post("/auth/logout", {
          headers: {
            cookie: `${SessionCookieName}=sess2`,
            "x-csrf-token": csrfToken,
          },
        });

        assert.strictEqual(res.status, 403);
        assert.strictEqual(yield* res.text, "Invalid CSRF token");
      }).pipe(Effect.provide(serve({}))),
    ); // getLogoutUrl intentionally NOT stubbed: must not be reached.
  });
});

// Mobile returns from auth via a custom-scheme deep link (e.g. denora://...).
// Unlike /auth/login (which needs an absolute request.url), /auth/callback
// derives its destination from the `state` param, so the app-scheme behavior IS
// exercisable under the Node test server.
describe("AuthRoutes mobile deep-link (app-scheme returnTo)", () => {
  const AppReturnTo = "denora://auth/callback";

  it.effect("callback delivers the sealed session in the deep-link fragment", () =>
    Effect.gen(function* () {
      const client = yield* HttpClient.HttpClient;
      const res = yield* client.get(
        `/auth/callback?code=abc&state=${encodeURIComponent(AppReturnTo)}`,
      );

      assert.strictEqual(res.status, 302);
      assert.isDefined(res.headers.location);
      const url = new URL(res.headers.location!);
      assert.strictEqual(url.protocol, "denora:");
      // Session is delivered in the fragment (apps read the hash; some OS link
      // handlers drop the query string).
      const fragment = new URLSearchParams(url.hash.slice(1));
      assert.strictEqual(fragment.get("authStatus"), "signed_in");
      assert.strictEqual(fragment.get("session"), "sealed");
      // The cookie is still set (harmless for the app; used by the web client).
      const cookie = getSetCookie(res, SessionCookieName);
      assert.isDefined(cookie);
      assert.strictEqual(cookie!.value, "sealed");
    }).pipe(
      Effect.provide(
        serve({
          authenticateWithCode: () =>
            Effect.succeed({ user: makeDenoraUser(), sealedSession: "sealed" }),
        }),
      ),
    ),
  );

  it.effect("callback errors land in the deep-link fragment, not the query", () =>
    Effect.gen(function* () {
      const client = yield* HttpClient.HttpClient;
      const res = yield* client.get(`/auth/callback?state=${encodeURIComponent(AppReturnTo)}`);

      assert.strictEqual(res.status, 302);
      const url = new URL(res.headers.location!);
      assert.strictEqual(url.protocol, "denora:");
      assert.strictEqual(url.search, "");
      const fragment = new URLSearchParams(url.hash.slice(1));
      assert.strictEqual(fragment.get("authError"), "callback_missing_code");
    }).pipe(Effect.provide(serve({}))),
  );
});
