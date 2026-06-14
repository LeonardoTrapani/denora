import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import * as HttpServer from "effect/unstable/http/HttpServer";
import { DenoraUser, Unauthorized } from "../../src/auth/User.ts";
import { Client } from "../../src/Client.ts";
import { Health } from "../../src/http/system/Schema.ts";
import { Routes } from "../../src/http/Routes.ts";
import { makeDenoraUser } from "../helpers/fixtures.ts";
import * as ServerConfigMock from "../helpers/ServerConfigMock.ts";
import * as TestServer from "../helpers/TestServer.ts";
import * as WorkOsAuthMock from "../helpers/WorkOsAuthMock.ts";

// =============================================================================
// SCHEMA
// =============================================================================

describe("schema: Health", () => {
  it("decodes { status: 'ok' }", () => {
    const health = Schema.decodeSync(Health)({ status: "ok" });
    assert.strictEqual(health.status, "ok");
    assert.instanceOf(health, Health);
  });

  it("rejects a non-'ok' status", () => {
    // `decodeUnknownOption` returns `None` when decoding fails, which lets us
    // assert the rejection without throwing in the test body.
    const decode = Schema.decodeUnknownOption(Health);
    assert.isTrue(Option.isNone(decode({ status: "other" })));
    assert.isTrue(Option.isNone(decode({ status: "OK" })));
  });

  it("rejects a missing status field", () => {
    assert.isTrue(Option.isNone(Schema.decodeUnknownOption(Health)({})));
  });

  it("throws on a bad status via decodeSync", () => {
    assert.throws(() => Schema.decodeSync(Health)({ status: "nope" } as never));
  });
});

describe("schema: DenoraUser", () => {
  const fullEncoded = {
    id: "00000000-0000-0000-0000-000000000001",
    workosUserId: "user_01HZX",
    email: "ada@example.com",
    emailVerified: true,
    name: "Ada Lovelace",
    firstName: "Ada",
    lastName: "Lovelace",
    profilePictureUrl: "https://example.com/ada.png",
    locale: "en-US",
    createdAt: "2026-01-01T00:00:00.000Z",
    updatedAt: "2026-01-02T00:00:00.000Z",
  };

  it("round-trips a fully-populated user (decode then encode)", () => {
    const user = Schema.decodeSync(DenoraUser)(fullEncoded);
    assert.instanceOf(user, DenoraUser);
    assert.strictEqual(user.email, "ada@example.com");
    assert.strictEqual(user.emailVerified, true);
    const encoded = Schema.encodeSync(DenoraUser)(user);
    assert.deepStrictEqual(encoded, fullEncoded);
  });

  it("round-trips with null nullable fields", () => {
    const encoded = {
      ...fullEncoded,
      name: null,
      firstName: null,
      lastName: null,
      profilePictureUrl: null,
      locale: null,
    };
    const user = Schema.decodeSync(DenoraUser)(encoded);
    assert.strictEqual(user.name, null);
    assert.strictEqual(user.firstName, null);
    assert.strictEqual(user.lastName, null);
    assert.strictEqual(user.profilePictureUrl, null);
    assert.strictEqual(user.locale, null);
    assert.deepStrictEqual(Schema.encodeSync(DenoraUser)(user), encoded);
  });

  it("rejects a missing required field", () => {
    const { email, ...withoutEmail } = fullEncoded;
    void email;
    assert.isTrue(Option.isNone(Schema.decodeUnknownOption(DenoraUser)(withoutEmail)));
  });

  it("rejects a wrong-typed field (emailVerified as string)", () => {
    const bad = { ...fullEncoded, emailVerified: "yes" };
    assert.isTrue(Option.isNone(Schema.decodeUnknownOption(DenoraUser)(bad)));
  });

  // Generated instances encode-then-decode back to an equal value: the schema
  // has no transformations, so this exercises structural stability of the
  // class shape across the codec boundary.
  it.effect.prop("encode then decode is identity", [DenoraUser], ([user]) =>
    Effect.sync(() => {
      const encoded = Schema.encodeSync(DenoraUser)(user);
      const decoded = Schema.decodeSync(DenoraUser)(encoded);
      assert.deepStrictEqual(Schema.encodeSync(DenoraUser)(decoded), encoded);
    }),
  );
});

describe("schema: Unauthorized", () => {
  it("constructs with _tag and message", () => {
    const err = new Unauthorized({ message: "Missing or invalid session" });
    assert.strictEqual(err._tag, "Unauthorized");
    assert.strictEqual(err.message, "Missing or invalid session");
    assert.instanceOf(err, Unauthorized);
  });

  it("round-trips through its own schema", () => {
    const err = new Unauthorized({ message: "nope" });
    const encoded = Schema.encodeSync(Unauthorized)(err);
    assert.strictEqual(encoded._tag, "Unauthorized");
    assert.strictEqual(encoded.message, "nope");
    const decoded = Schema.decodeSync(Unauthorized)(encoded);
    assert.strictEqual(decoded._tag, "Unauthorized");
    assert.strictEqual(decoded.message, "nope");
  });
});

// =============================================================================
// CLIENT
// =============================================================================

describe("client: makeDenoraUrlBuilder", () => {
  // Both `System` (health) and `Account` (me) groups are `topLevel: true`, so
  // the url builder exposes the endpoint names directly at the top level.
  it("builds absolute urls for the top-level endpoints", () => {
    const build = Client.makeDenoraUrlBuilder("http://api.test");
    assert.strictEqual(build.health(), "http://api.test/health");
    assert.strictEqual(build.me(), "http://api.test/me");
  });

  it("preserves a base path and normalizes trailing slashes", () => {
    const build = Client.makeDenoraUrlBuilder("http://api.test/");
    assert.strictEqual(build.health(), "http://api.test/health");
  });

  it("accepts a URL instance as the base", () => {
    const build = Client.makeDenoraUrlBuilder(new URL("https://app.denora.me"));
    assert.strictEqual(build.health(), "https://app.denora.me/health");
    assert.strictEqual(build.me(), "https://app.denora.me/me");
  });
});

describe("client: makeDenoraAuthUrls", () => {
  // The auth routes are plain HttpRouter routes (not part of the HttpApi), so
  // the client exposes them as hand-built urls. returnTo is url-encoded so app
  // deep links (denora://...) survive intact.
  it("builds the auth endpoint urls and encodes returnTo, normalizing the base", () => {
    const urls = Client.makeDenoraAuthUrls("http://api.test/");
    assert.strictEqual(urls.csrfToken(), "http://api.test/auth/csrf-token");
    assert.strictEqual(
      urls.login("denora://auth/callback"),
      `http://api.test/auth/login?returnTo=${encodeURIComponent("denora://auth/callback")}`,
    );
    assert.strictEqual(
      urls.logout("https://app.denora.me/"),
      `http://api.test/auth/logout?returnTo=${encodeURIComponent("https://app.denora.me/")}`,
    );
  });
});

const appLayer = Routes.layer.pipe(
  Layer.provide(
    WorkOsAuthMock.layer({
      authenticateSession: (credential) =>
        Redacted.value(credential) === "valid-session"
          ? Effect.succeed({ user: makeDenoraUser() })
          : Effect.fail(new Unauthorized({ message: "Missing or invalid session" })),
    }),
  ),
  Layer.provide(ServerConfigMock.layer()),
);

const serverLayer = TestServer.layer(appLayer);

describe("client: makeDenoraClient (real round-trip)", () => {
  it.effect("calls the health endpoint against the served app", () =>
    Effect.gen(function* () {
      const server = yield* HttpServer.HttpServer;
      const address = server.address;
      assert.strictEqual(address._tag, "TcpAddress");
      // Narrowed by the assertion above; read the bound ephemeral port.
      const port = address._tag === "TcpAddress" ? address.port : 0;
      assert.isTrue(port > 0);

      const baseUrl = `http://127.0.0.1:${port}`;
      const client = yield* Client.makeDenoraClient(baseUrl);
      const health = yield* client.health();
      assert.deepStrictEqual(health, new Health({ status: "ok" }));
      assert.strictEqual(health.status, "ok");
    }).pipe(Effect.provide(serverLayer)),
  );

  it.effect("exposes the typed endpoint functions on the client", () =>
    Effect.gen(function* () {
      const server = yield* HttpServer.HttpServer;
      const port = server.address._tag === "TcpAddress" ? server.address.port : 0;
      const client = yield* Client.makeDenoraClient(`http://127.0.0.1:${port}`);
      assert.isFunction(client.health);
      assert.isFunction(client.me);
    }).pipe(Effect.provide(serverLayer)),
  );

  it.effect("accepts a custom httpClientLayer and still decodes Health", () =>
    Effect.gen(function* () {
      const server = yield* HttpServer.HttpServer;
      const port = server.address._tag === "TcpAddress" ? server.address.port : 0;
      // Supply a fresh FetchHttpClient via the option path (rather than the
      // default) to exercise `DenoraClientOptions.httpClientLayer`.
      const client = yield* Client.makeDenoraClient(`http://127.0.0.1:${port}`, {
        httpClientLayer: FetchHttpClient.layer,
      });
      const health = yield* client.health();
      assert.deepStrictEqual(health, new Health({ status: "ok" }));
    }).pipe(Effect.provide(serverLayer)),
  );
});
