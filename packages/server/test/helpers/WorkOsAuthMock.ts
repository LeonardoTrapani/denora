import type { WorkOS } from "@workos-inc/node";
import * as Layer from "effect/Layer";
import { WorkOsAuth } from "../../src/auth/WorkOsAuth.ts";

const notStubbed = (method: string) => (): never => {
  throw new Error(`WorkOsAuth.${method} was called but not stubbed in this test`);
};

// Builds a `WorkOsAuth.Interface` whose methods throw unless a test stubs them,
// so an unexpected call surfaces loudly instead of silently returning garbage.
export const make = (overrides: Partial<WorkOsAuth.Interface>): WorkOsAuth.Interface => ({
  client: {} as WorkOS,
  getAuthorizationUrl: notStubbed("getAuthorizationUrl"),
  authenticateWithCode: notStubbed("authenticateWithCode"),
  getLogoutUrl: notStubbed("getLogoutUrl"),
  authenticateSession: notStubbed("authenticateSession"),
  ...overrides,
});

export const layer = (
  overrides: Partial<WorkOsAuth.Interface> = {},
): Layer.Layer<WorkOsAuth.Service> =>
  Layer.succeed(WorkOsAuth.Service, WorkOsAuth.Service.of(make(overrides)));
