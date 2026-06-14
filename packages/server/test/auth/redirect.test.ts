import { assert, describe, it } from "@effect/vitest";
import { AuthRoutes } from "../../src/http/auth/Routes.ts";
import * as ServerConfigMock from "../helpers/ServerConfigMock.ts";

const auth = ServerConfigMock.testAuth;
const fallback = "http://localhost:3000"; // auth.webOrigins[0]

describe("AuthRoutes.redirectToAllowedReturnTo", () => {
  it("returns the normalized url for an allowed origin (bare origin gains trailing slash)", () => {
    assert.strictEqual(
      AuthRoutes.redirectToAllowedReturnTo(auth, "https://app.denora.me"),
      "https://app.denora.me/",
    );
  });

  it("returns the normalized url for an allowed origin with a path", () => {
    assert.strictEqual(
      AuthRoutes.redirectToAllowedReturnTo(auth, "https://app.denora.me/path"),
      "https://app.denora.me/path",
    );
  });

  it("allows the other configured origin too", () => {
    assert.strictEqual(
      AuthRoutes.redirectToAllowedReturnTo(auth, "http://localhost:3000/x"),
      "http://localhost:3000/x",
    );
  });

  it("allows an app-scheme deep link whose scheme is configured", () => {
    // auth.appRedirectSchemes includes "denora" — mobile returns here via a
    // custom-scheme deep link rather than an http origin.
    assert.strictEqual(
      AuthRoutes.redirectToAllowedReturnTo(auth, "denora://auth/callback"),
      new URL("denora://auth/callback").toString(),
    );
  });

  it("falls back for an app scheme that is not configured", () => {
    assert.strictEqual(AuthRoutes.redirectToAllowedReturnTo(auth, "evilapp://x"), fallback);
  });

  it("falls back for a disallowed absolute origin", () => {
    assert.strictEqual(AuthRoutes.redirectToAllowedReturnTo(auth, "https://evil.com/x"), fallback);
  });

  it("falls back for a null candidate", () => {
    assert.strictEqual(AuthRoutes.redirectToAllowedReturnTo(auth, null), fallback);
  });

  it("resolves a relative path against the fallback origin", () => {
    assert.strictEqual(
      AuthRoutes.redirectToAllowedReturnTo(auth, "/dashboard"),
      "http://localhost:3000/dashboard",
    );
  });

  it("rejects a protocol-relative url (//evil.com) and falls back", () => {
    assert.strictEqual(AuthRoutes.redirectToAllowedReturnTo(auth, "//evil.com/x"), fallback);
  });

  it("falls back for junk that is neither a URL nor an absolute path", () => {
    assert.strictEqual(AuthRoutes.redirectToAllowedReturnTo(auth, "not a url"), fallback);
  });
});

describe("AuthRoutes.isAllowedAppReturnTo", () => {
  it("accepts a configured app scheme", () => {
    assert.isTrue(AuthRoutes.isAllowedAppReturnTo(auth, "denora://whatever"));
  });

  it("rejects web origins, unconfigured schemes, and non-urls", () => {
    assert.isFalse(AuthRoutes.isAllowedAppReturnTo(auth, "https://app.denora.me"));
    assert.isFalse(AuthRoutes.isAllowedAppReturnTo(auth, "evilapp://x"));
    assert.isFalse(AuthRoutes.isAllowedAppReturnTo(auth, "not a url"));
  });
});

describe("AuthRoutes.withAuthError", () => {
  it("appends ?authError=code to a valid web url", () => {
    assert.strictEqual(
      AuthRoutes.withAuthError(auth, "https://app.denora.me/", "login_failed"),
      "https://app.denora.me/?authError=login_failed",
    );
  });

  it("preserves an existing query and overwrites a duplicate authError", () => {
    assert.strictEqual(
      AuthRoutes.withAuthError(auth, "https://app.denora.me/p?foo=1", "boom"),
      "https://app.denora.me/p?foo=1&authError=boom",
    );
    assert.strictEqual(
      AuthRoutes.withAuthError(auth, "https://app.denora.me/p?authError=old", "new"),
      "https://app.denora.me/p?authError=new",
    );
  });

  it("returns a non-parseable destination unchanged", () => {
    assert.strictEqual(AuthRoutes.withAuthError(auth, "not a url", "x"), "not a url");
  });

  it("puts the error in the fragment (not the query) for an app-scheme destination", () => {
    // Mobile reads auth results from the deep-link fragment; query params can be
    // dropped by some OS link handlers.
    const result = AuthRoutes.withAuthError(auth, "denora://auth/callback", "login_failed");
    const url = new URL(result);
    assert.strictEqual(url.protocol, "denora:");
    assert.strictEqual(url.search, "");
    const params = new URLSearchParams(url.hash.slice(1));
    assert.strictEqual(params.get("authError"), "login_failed");
  });
});

describe("AuthRoutes.withMobileSession", () => {
  it("adds signed-in status + sealed session to the fragment for an app-scheme destination", () => {
    const result = AuthRoutes.withMobileSession(auth, "denora://auth/callback", "sealed-xyz");
    const url = new URL(result);
    const params = new URLSearchParams(url.hash.slice(1));
    assert.strictEqual(params.get("authStatus"), "signed_in");
    assert.strictEqual(params.get("session"), "sealed-xyz");
  });

  it("leaves a web destination untouched (the cookie carries the session there)", () => {
    assert.strictEqual(
      AuthRoutes.withMobileSession(auth, "https://app.denora.me/welcome", "sealed-xyz"),
      "https://app.denora.me/welcome",
    );
  });

  it("returns the destination unchanged when there is no sealed session", () => {
    assert.strictEqual(
      AuthRoutes.withMobileSession(auth, "denora://auth/callback", undefined),
      "denora://auth/callback",
    );
  });
});
