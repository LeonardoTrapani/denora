import type { User as WorkOsUser } from "@workos-inc/node";
import { DenoraUser } from "../../src/auth/User.ts";

// A fully-populated WorkOS user. Tests override only the fields they assert on.
export const makeWorkOsUser = (overrides: Partial<WorkOsUser> = {}): WorkOsUser =>
  ({
    object: "user",
    id: "user_01HZX",
    email: "ada@example.com",
    emailVerified: true,
    name: "Ada Lovelace",
    firstName: "Ada",
    lastName: "Lovelace",
    profilePictureUrl: null,
    locale: null,
    lastSignInAt: "2026-06-01T00:00:00.000Z",
    createdAt: "2026-01-01T00:00:00.000Z",
    updatedAt: "2026-01-01T00:00:00.000Z",
    ...overrides,
  }) as WorkOsUser;

export const makeDenoraUser = (overrides: Partial<DenoraUser> = {}): DenoraUser =>
  new DenoraUser({
    id: "00000000-0000-0000-0000-000000000001",
    workosUserId: "user_01HZX",
    email: "ada@example.com",
    emailVerified: true,
    name: "Ada Lovelace",
    firstName: "Ada",
    lastName: "Lovelace",
    profilePictureUrl: null,
    locale: null,
    createdAt: "2026-01-01T00:00:00.000Z",
    updatedAt: "2026-01-01T00:00:00.000Z",
    ...overrides,
  });
