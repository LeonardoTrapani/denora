import { assert, describe, it } from "@effect/vitest";
import { UserSync } from "../../src/auth/UserSync.ts";
import type { schema } from "../../src/persistence/schema.ts";
import { makeWorkOsUser } from "../helpers/fixtures.ts";

type UserRow = typeof schema.users.$inferSelect;

// A full users row. Tests override only the staleness-relevant columns
// (workosUpdatedAt / workosDeletedAt); everything else is filler.
const makeRow = (
  overrides: Partial<Pick<UserRow, "workosUpdatedAt" | "workosDeletedAt">> = {},
): UserRow => ({
  id: "00000000-0000-0000-0000-000000000001",
  workosUserId: "user_01HZX",
  email: "ada@example.com",
  emailVerified: true,
  name: "Ada Lovelace",
  firstName: "Ada",
  lastName: "Lovelace",
  profilePictureUrl: null,
  locale: null,
  lastSignInAt: "2026-06-01T00:00:00.000Z",
  workosCreatedAt: "2026-01-01T00:00:00.000Z",
  workosUpdatedAt: "2026-01-01T00:00:00.000Z",
  deletedAt: null,
  workosDeletedAt: null,
  createdAt: "2026-01-01T00:00:00.000Z",
  updatedAt: "2026-01-01T00:00:00.000Z",
  ...overrides,
});

describe("UserSync.isStaleActiveProjection", () => {
  it("TRUE when existing.workosUpdatedAt is newer than the incoming update", () => {
    const existing = makeRow({ workosUpdatedAt: "2026-06-10T00:00:00.000Z" });
    const incoming = makeWorkOsUser({ updatedAt: "2026-06-01T00:00:00.000Z" });
    assert.isTrue(UserSync.isStaleActiveProjection(existing, incoming));
  });

  it("FALSE for a fresh incoming update (existing older, not deleted)", () => {
    const existing = makeRow({ workosUpdatedAt: "2026-06-01T00:00:00.000Z" });
    const incoming = makeWorkOsUser({ updatedAt: "2026-06-10T00:00:00.000Z" });
    assert.isFalse(UserSync.isStaleActiveProjection(existing, incoming));
  });

  it("FALSE when timestamps are exactly equal and not deleted (> is strict)", () => {
    const existing = makeRow({ workosUpdatedAt: "2026-06-01T00:00:00.000Z" });
    const incoming = makeWorkOsUser({ updatedAt: "2026-06-01T00:00:00.000Z" });
    assert.isFalse(UserSync.isStaleActiveProjection(existing, incoming));
  });

  it("TRUE when a deletion projection exists at or after the incoming update", () => {
    // existing.workosUpdatedAt older than incoming, but a delete is recorded.
    const existing = makeRow({
      workosUpdatedAt: "2026-06-01T00:00:00.000Z",
      workosDeletedAt: "2026-06-12T00:00:00.000Z",
    });
    const incoming = makeWorkOsUser({ updatedAt: "2026-06-10T00:00:00.000Z" });
    assert.isTrue(UserSync.isStaleActiveProjection(existing, incoming));
  });

  it("TRUE when the recorded deletion equals the incoming update (>= boundary)", () => {
    const existing = makeRow({
      workosUpdatedAt: "2026-06-01T00:00:00.000Z",
      workosDeletedAt: "2026-06-10T00:00:00.000Z",
    });
    const incoming = makeWorkOsUser({ updatedAt: "2026-06-10T00:00:00.000Z" });
    assert.isTrue(UserSync.isStaleActiveProjection(existing, incoming));
  });

  it("FALSE when a deletion exists but predates the incoming update", () => {
    const existing = makeRow({
      workosUpdatedAt: "2026-06-01T00:00:00.000Z",
      workosDeletedAt: "2026-06-05T00:00:00.000Z",
    });
    const incoming = makeWorkOsUser({ updatedAt: "2026-06-10T00:00:00.000Z" });
    assert.isFalse(UserSync.isStaleActiveProjection(existing, incoming));
  });

  it("FALSE when workosDeletedAt is null even if it would lexically pass", () => {
    // Guards against treating a null deletion as a delete event.
    const existing = makeRow({
      workosUpdatedAt: "2026-06-01T00:00:00.000Z",
      workosDeletedAt: null,
    });
    const incoming = makeWorkOsUser({ updatedAt: "2026-06-01T00:00:00.000Z" });
    assert.isFalse(UserSync.isStaleActiveProjection(existing, incoming));
  });
});

describe("UserSync.isStaleDeletedProjection", () => {
  it("TRUE when existing.workosUpdatedAt is newer than the incoming update", () => {
    const existing = makeRow({ workosUpdatedAt: "2026-06-12T00:00:00.000Z" });
    const incoming = makeWorkOsUser({ updatedAt: "2026-06-10T00:00:00.000Z" });
    assert.isTrue(
      UserSync.isStaleDeletedProjection(existing, incoming, "2026-06-11T00:00:00.000Z"),
    );
  });

  it("FALSE for a fresh incoming delete (existing older, no prior delete)", () => {
    const existing = makeRow({
      workosUpdatedAt: "2026-06-01T00:00:00.000Z",
      workosDeletedAt: null,
    });
    const incoming = makeWorkOsUser({ updatedAt: "2026-06-05T00:00:00.000Z" });
    assert.isFalse(
      UserSync.isStaleDeletedProjection(existing, incoming, "2026-06-10T00:00:00.000Z"),
    );
  });

  it("TRUE when a recorded deletion is at or after the incoming deletedAt arg", () => {
    // The delete guard compares against the 3rd arg (deletedAt), not updatedAt.
    const existing = makeRow({
      workosUpdatedAt: "2026-06-01T00:00:00.000Z",
      workosDeletedAt: "2026-06-15T00:00:00.000Z",
    });
    const incoming = makeWorkOsUser({ updatedAt: "2026-06-20T00:00:00.000Z" });
    assert.isTrue(
      UserSync.isStaleDeletedProjection(existing, incoming, "2026-06-10T00:00:00.000Z"),
    );
  });

  it("TRUE when the recorded deletion equals the incoming deletedAt (>= boundary)", () => {
    const existing = makeRow({
      workosUpdatedAt: "2026-06-01T00:00:00.000Z",
      workosDeletedAt: "2026-06-10T00:00:00.000Z",
    });
    const incoming = makeWorkOsUser({ updatedAt: "2026-06-20T00:00:00.000Z" });
    assert.isTrue(
      UserSync.isStaleDeletedProjection(existing, incoming, "2026-06-10T00:00:00.000Z"),
    );
  });

  it("FALSE when a prior deletion predates the incoming deletedAt", () => {
    const existing = makeRow({
      workosUpdatedAt: "2026-06-01T00:00:00.000Z",
      workosDeletedAt: "2026-06-05T00:00:00.000Z",
    });
    const incoming = makeWorkOsUser({ updatedAt: "2026-06-20T00:00:00.000Z" });
    assert.isFalse(
      UserSync.isStaleDeletedProjection(existing, incoming, "2026-06-10T00:00:00.000Z"),
    );
  });

  it("delete guard ignores updatedAt: deletedAt arg drives the comparison", () => {
    // workosDeletedAt < updatedAt but >= deletedAt -> still stale.
    const existing = makeRow({
      workosUpdatedAt: "2026-06-01T00:00:00.000Z",
      workosDeletedAt: "2026-06-08T00:00:00.000Z",
    });
    const incoming = makeWorkOsUser({ updatedAt: "2026-06-20T00:00:00.000Z" });
    assert.isTrue(
      UserSync.isStaleDeletedProjection(existing, incoming, "2026-06-08T00:00:00.000Z"),
    );
  });

  it("FALSE when workosDeletedAt is null even if deletedAt would lexically pass", () => {
    const existing = makeRow({
      workosUpdatedAt: "2026-06-01T00:00:00.000Z",
      workosDeletedAt: null,
    });
    const incoming = makeWorkOsUser({ updatedAt: "2026-06-20T00:00:00.000Z" });
    assert.isFalse(
      UserSync.isStaleDeletedProjection(existing, incoming, "2026-06-01T00:00:00.000Z"),
    );
  });
});
