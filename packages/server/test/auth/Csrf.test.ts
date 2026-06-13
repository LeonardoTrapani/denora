import { assert, describe, it } from "@effect/vitest";
import * as Redacted from "effect/Redacted";
import { AuthRoutes } from "../../src/http/auth/Routes.ts";
import type { ServerConfig } from "../../src/config/ServerConfig.ts";
import * as ServerConfigMock from "../helpers/ServerConfigMock.ts";

const auth = ServerConfigMock.testAuth;

// Same secret value as testAuth but a distinct Redacted instance: proves the
// signature is keyed on the secret value, not on object identity.
const sameSecretAuth: ServerConfig.Auth = {
  ...auth,
  csrfSecret: Redacted.make(Redacted.value(auth.csrfSecret)),
};

const otherSecretAuth: ServerConfig.Auth = {
  ...auth,
  csrfSecret: Redacted.make("a-completely-different-csrf-secret"),
};

const craftToken = (
  options: ServerConfig.Auth,
  issuedAtMs: number,
  nonce: string,
  session: string | undefined,
) => {
  const issuedAt = issuedAtMs.toString(36);
  const sig = AuthRoutes.signCsrfToken(options.csrfSecret, issuedAt, nonce, session);
  return `${issuedAt}.${nonce}.${sig}`;
};

describe("CSRF: round-trip", () => {
  it("createCsrfToken then isValidCsrfToken is true with a session cookie", () => {
    const token = AuthRoutes.createCsrfToken(auth, "sessA");
    assert.isTrue(AuthRoutes.isValidCsrfToken(auth, token, "sessA"));
  });

  it("createCsrfToken then isValidCsrfToken is true without a session cookie", () => {
    const token = AuthRoutes.createCsrfToken(auth, undefined);
    assert.isTrue(AuthRoutes.isValidCsrfToken(auth, token, undefined));
  });

  it("produces the documented three-segment format", () => {
    const token = AuthRoutes.createCsrfToken(auth, "sessA");
    const parts = token.split(".");
    assert.strictEqual(parts.length, 3);
    const [issuedAt, nonce, signature] = parts;
    // issuedAt is base36 of Date.now() -> finite when parsed back.
    assert.isTrue(Number.isFinite(Number.parseInt(issuedAt!, 36)));
    assert.isNotEmpty(nonce!);
    assert.isNotEmpty(signature!);
  });

  it("issues distinct tokens across calls (random nonce)", () => {
    const a = AuthRoutes.createCsrfToken(auth, "sessA");
    const b = AuthRoutes.createCsrfToken(auth, "sessA");
    assert.notStrictEqual(a, b);
  });
});

describe("CSRF: session binding", () => {
  it("token for sessA is invalid when validated with sessB", () => {
    const token = AuthRoutes.createCsrfToken(auth, "sessA");
    assert.isFalse(AuthRoutes.isValidCsrfToken(auth, token, "sessB"));
  });

  it("token for sessA is invalid when validated with undefined", () => {
    const token = AuthRoutes.createCsrfToken(auth, "sessA");
    assert.isFalse(AuthRoutes.isValidCsrfToken(auth, token, undefined));
  });

  it("token for no-session is invalid when validated with a session", () => {
    const token = AuthRoutes.createCsrfToken(auth, undefined);
    assert.isFalse(AuthRoutes.isValidCsrfToken(auth, token, "sessA"));
  });
});

describe("CSRF: tampering", () => {
  it("flipping a char in the signature segment invalidates the token", () => {
    const token = AuthRoutes.createCsrfToken(auth, "sessA");
    const [issuedAt, nonce, signature] = token.split(".");
    const firstChar = signature!.charAt(0);
    // base64url alphabet: pick a guaranteed-different replacement.
    const flipped = (firstChar === "A" ? "B" : "A") + signature!.slice(1);
    const tampered = `${issuedAt}.${nonce}.${flipped}`;
    assert.isFalse(AuthRoutes.isValidCsrfToken(auth, tampered, "sessA"));
  });

  it("mutating the nonce invalidates the token", () => {
    const token = AuthRoutes.createCsrfToken(auth, "sessA");
    const [issuedAt, nonce, signature] = token.split(".");
    const mutatedNonce = (nonce!.charAt(0) === "A" ? "B" : "A") + nonce!.slice(1);
    const tampered = `${issuedAt}.${mutatedNonce}.${signature}`;
    assert.isFalse(AuthRoutes.isValidCsrfToken(auth, tampered, "sessA"));
  });
});

describe("CSRF: malformed tokens", () => {
  it("null token is invalid", () => {
    assert.isFalse(AuthRoutes.isValidCsrfToken(auth, null, "sessA"));
  });

  it("empty string is invalid", () => {
    assert.isFalse(AuthRoutes.isValidCsrfToken(auth, "", "sessA"));
  });

  it("single segment is invalid", () => {
    assert.isFalse(AuthRoutes.isValidCsrfToken(auth, "onlyonepart", "sessA"));
  });

  it("missing signature segment (a.b) is invalid", () => {
    assert.isFalse(AuthRoutes.isValidCsrfToken(auth, "a.b", "sessA"));
  });

  it("extra segment (a.b.c.d) is invalid", () => {
    assert.isFalse(AuthRoutes.isValidCsrfToken(auth, "a.b.c.d", "sessA"));
  });

  it("issuedAt not base36-parseable is invalid", () => {
    // "$$$" -> Number.parseInt("$$$", 36) is NaN -> not finite.
    const issuedAt = "$$$";
    const nonce = "abc";
    const sig = AuthRoutes.signCsrfToken(auth.csrfSecret, issuedAt, nonce, "sessA");
    const token = `${issuedAt}.${nonce}.${sig}`;
    assert.isFalse(AuthRoutes.isValidCsrfToken(auth, token, "sessA"));
  });
});

describe("CSRF: secret", () => {
  it("a token signed with a different secret is invalid", () => {
    const token = AuthRoutes.createCsrfToken(otherSecretAuth, "sessA");
    assert.isFalse(AuthRoutes.isValidCsrfToken(auth, token, "sessA"));
  });

  it("validation depends on the secret value, not the Redacted identity", () => {
    const token = AuthRoutes.createCsrfToken(auth, "sessA");
    assert.isTrue(AuthRoutes.isValidCsrfToken(sameSecretAuth, token, "sessA"));
  });
});

describe("CSRF: expiry", () => {
  it("a token older than the TTL is invalid", () => {
    const oldIssuedAtMs = Date.now() - AuthRoutes.CsrfTokenTtlMs - 1000;
    const token = craftToken(auth, oldIssuedAtMs, "abc", "sessA");
    assert.isFalse(AuthRoutes.isValidCsrfToken(auth, token, "sessA"));
  });

  it("a token issued just inside the TTL is still valid", () => {
    // Leave generous slack so wall-clock drift between craft and validate
    // can't push age past the TTL.
    const recentIssuedAtMs = Date.now() - (AuthRoutes.CsrfTokenTtlMs - 60_000);
    const token = craftToken(auth, recentIssuedAtMs, "abc", "sessA");
    assert.isTrue(AuthRoutes.isValidCsrfToken(auth, token, "sessA"));
  });

  it("a token issued in the future (negative age) is invalid", () => {
    const futureIssuedAtMs = Date.now() + 60 * 60 * 1000;
    const token = craftToken(auth, futureIssuedAtMs, "abc", "sessA");
    assert.isFalse(AuthRoutes.isValidCsrfToken(auth, token, "sessA"));
  });
});
