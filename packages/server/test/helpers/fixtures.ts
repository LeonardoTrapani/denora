import { DenoraUser } from "../../src/auth/User.ts";

// A fully-populated authenticated user. Tests override only the fields they
// assert on.
export const makeDenoraUser = (overrides: Partial<DenoraUser> = {}): DenoraUser =>
  new DenoraUser({
    id: "00000000-0000-0000-0000-000000000001",
    email: "ada@example.com",
    emailVerified: true,
    name: "Ada Lovelace",
    image: null,
    createdAt: "2026-01-01T00:00:00.000Z",
    updatedAt: "2026-01-02T00:00:00.000Z",
    ...overrides,
  });
