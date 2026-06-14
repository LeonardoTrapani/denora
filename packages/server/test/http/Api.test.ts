import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as HttpClient from "effect/unstable/http/HttpClient";
import * as HttpApiClient from "effect/unstable/httpapi/HttpApiClient";
import { Unauthorized } from "../../src/auth/User.ts";
import type { WorkOsAuth } from "../../src/auth/WorkOsAuth.ts";
import { Api } from "../../src/http/Api.ts";
import { Routes } from "../../src/http/Routes.ts";
import { makeDenoraUser } from "../helpers/fixtures.ts";
import * as ServerConfigMock from "../helpers/ServerConfigMock.ts";
import * as TestServer from "../helpers/TestServer.ts";
import * as WorkOsAuthMock from "../helpers/WorkOsAuthMock.ts";

// The /me handler runs behind the Authorization security middleware, which
// resolves the wos-session cookie via WorkOsAuth.authenticateSession. We swap
// that single method per scenario; the seal/unseal + WorkOS network path is out
// of scope here, so the mock returns already-synced DenoraUsers directly.
const serveWith = (authenticateSession: WorkOsAuth.Interface["authenticateSession"]) =>
  TestServer.layer(
    Routes.layer.pipe(
      Layer.provide(WorkOsAuthMock.layer({ authenticateSession })),
      Layer.provide(ServerConfigMock.layer()),
    ),
  );

const validUser = makeDenoraUser();

const validSessionLayer = serveWith((credential) =>
  Redacted.value(credential) === "valid"
    ? Effect.succeed({ user: validUser })
    : Effect.fail(new Unauthorized({ message: "Missing or invalid session" })),
);

const rotatingSessionLayer = serveWith((credential) =>
  Redacted.value(credential) === "valid"
    ? Effect.succeed({ user: validUser, sealedSession: "rotated" })
    : Effect.fail(new Unauthorized({ message: "Missing or invalid session" })),
);

describe("Api http surface", () => {
  describe("System group (topLevel) GET /health", () => {
    it.effect("returns 200 with json { status: 'ok' }", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const res = yield* client.get("/health");
        assert.strictEqual(res.status, 200);
        assert.deepStrictEqual(yield* res.json, { status: "ok" });
      }).pipe(Effect.provide(validSessionLayer)),
    );

    // System is a topLevel group, so the generated client exposes the endpoint
    // directly on the root (client.health()) rather than client.System.health().
    it.effect("is reachable via the generated typed client and decodes to a Health", () =>
      Effect.gen(function* () {
        const client = yield* HttpApiClient.make(Api.DenoraApi);
        const health = yield* client.health();
        assert.strictEqual(health.status, "ok");
      }).pipe(Effect.provide(validSessionLayer)),
    );
  });

  describe("Account group GET /me", () => {
    it.effect("with no cookie is 401", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const res = yield* client.get("/me");
        assert.strictEqual(res.status, 401);
      }).pipe(Effect.provide(validSessionLayer)),
    );

    it.effect("with an invalid cookie credential is 401", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const res = yield* client.get("/me", {
          headers: { cookie: "wos-session=bogus" },
        });
        assert.strictEqual(res.status, 401);
      }).pipe(Effect.provide(validSessionLayer)),
    );

    it.effect("with a valid cookie returns 200 and the DenoraUser body", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const res = yield* client.get("/me", {
          headers: { cookie: "wos-session=valid" },
        });
        assert.strictEqual(res.status, 200);
        const body = yield* res.json;
        assert.deepStrictEqual(body, {
          id: validUser.id,
          workosUserId: validUser.workosUserId,
          email: validUser.email,
          emailVerified: validUser.emailVerified,
          name: validUser.name,
          firstName: validUser.firstName,
          lastName: validUser.lastName,
          profilePictureUrl: validUser.profilePictureUrl,
          locale: validUser.locale,
          createdAt: validUser.createdAt,
          updatedAt: validUser.updatedAt,
        });
      }).pipe(Effect.provide(validSessionLayer)),
    );

    // When authenticateSession rotates the session, Authorization appends a
    // pre-response handler that re-sets the wos-session cookie so the browser
    // picks up the refreshed sealed session.
    it.effect("sets a refreshed wos-session cookie when the session rotates", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const res = yield* client.get("/me", {
          headers: { cookie: "wos-session=valid" },
        });
        assert.strictEqual(res.status, 200);
        const setCookie = res.headers["set-cookie"];
        assert.isDefined(setCookie);
        const cookieHeader = Array.isArray(setCookie) ? setCookie.join("; ") : String(setCookie);
        assert.include(cookieHeader, "wos-session");
        assert.include(cookieHeader, "rotated");
      }).pipe(Effect.provide(rotatingSessionLayer)),
    );

    it.effect("does NOT set a wos-session cookie when the session is not rotated", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const res = yield* client.get("/me", {
          headers: { cookie: "wos-session=valid" },
        });
        assert.strictEqual(res.status, 200);
        assert.isUndefined(res.headers["set-cookie"]);
      }).pipe(Effect.provide(validSessionLayer)),
    );
  });
});
