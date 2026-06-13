import { assert, describe, it } from "@effect/vitest";
import { AuthRoutes } from "../../src/http/auth/Routes.ts";
import * as ServerConfigMock from "../helpers/ServerConfigMock.ts";

const auth = ServerConfigMock.testAuth;
const fallback = "http://localhost:3000"; // auth.webOrigins[0]

describe("AuthRoutes.redirectToAllowedWebOrigin", () => {
  it("returns the normalized url for an allowed origin (bare origin gains trailing slash)", () => {
    assert.strictEqual(
      AuthRoutes.redirectToAllowedWebOrigin(auth, "https://app.denora.me"),
      "https://app.denora.me/",
    );
  });

  it("returns the normalized url for an allowed origin with a path", () => {
    assert.strictEqual(
      AuthRoutes.redirectToAllowedWebOrigin(auth, "https://app.denora.me/path"),
      "https://app.denora.me/path",
    );
  });

  it("allows the other configured origin too", () => {
    assert.strictEqual(
      AuthRoutes.redirectToAllowedWebOrigin(auth, "http://localhost:3000/x"),
      "http://localhost:3000/x",
    );
  });

  it("falls back for a disallowed absolute origin", () => {
    assert.strictEqual(AuthRoutes.redirectToAllowedWebOrigin(auth, "https://evil.com/x"), fallback);
  });

  it("falls back for a null candidate", () => {
    assert.strictEqual(AuthRoutes.redirectToAllowedWebOrigin(auth, null), fallback);
  });

  it("resolves a relative path against the fallback origin", () => {
    assert.strictEqual(
      AuthRoutes.redirectToAllowedWebOrigin(auth, "/dashboard"),
      new URL("/dashboard", fallback).toString(),
    );
    assert.strictEqual(
      AuthRoutes.redirectToAllowedWebOrigin(auth, "/dashboard"),
      "http://localhost:3000/dashboard",
    );
  });

  it("rejects a protocol-relative url (//evil.com) and falls back", () => {
    assert.strictEqual(AuthRoutes.redirectToAllowedWebOrigin(auth, "//evil.com/x"), fallback);
  });

  it("falls back for junk that is neither a URL nor an absolute path", () => {
    assert.strictEqual(AuthRoutes.redirectToAllowedWebOrigin(auth, "not a url"), fallback);
  });
});

describe("AuthRoutes.withAuthError", () => {
  it("appends ?authError=code to a valid url", () => {
    assert.strictEqual(
      AuthRoutes.withAuthError("https://app.denora.me/", "login_failed"),
      "https://app.denora.me/?authError=login_failed",
    );
  });

  it("preserves an existing query and overwrites a duplicate authError", () => {
    assert.strictEqual(
      AuthRoutes.withAuthError("https://app.denora.me/p?foo=1", "boom"),
      "https://app.denora.me/p?foo=1&authError=boom",
    );
    assert.strictEqual(
      AuthRoutes.withAuthError("https://app.denora.me/p?authError=old", "new"),
      "https://app.denora.me/p?authError=new",
    );
  });

  it("returns a non-parseable destination unchanged", () => {
    assert.strictEqual(AuthRoutes.withAuthError("not a url", "x"), "not a url");
  });
});
