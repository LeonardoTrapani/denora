import { AsyncLocalStorage } from "node:async_hooks";
import type * as Context from "effect/Context";

/**
 * better-auth drives the database through a plain-Promise adapter and has no
 * handle on the Effect runtime. Our adapter, however, runs queries against the
 * alchemy-managed effect-postgres `Db` client, whose query Effects resolve and
 * reuse the pooled Hyperdrive connection from the per-request `ExecutionContext`
 * (scope + cache) at runtime — alchemy builds the worker service layer once at
 * init and runs each request under a fresh `ExecutionContext`.
 *
 * So at the start of every auth request we capture the full Effect context and
 * stash it here; the adapter reads it back (`current`) and runs its queries with
 * it, so they hit the same request connection. `ExecutionContext` is type-erased
 * to `never` (as alchemy itself does for its proxy queries) but always present at
 * runtime, so no cast is needed. This lets a singleton better-auth instance use a
 * request-scoped connection without rebuilding better-auth per request.
 */
export type AuthRequestContext = Context.Context<never>;

const storage = new AsyncLocalStorage<AuthRequestContext>();

export const runWith = <A>(context: AuthRequestContext, thunk: () => Promise<A>): Promise<A> =>
  storage.run(context, thunk);

export const current = (): AuthRequestContext => {
  const context = storage.getStore();
  if (context === undefined) {
    throw new Error(
      "better-auth adapter used outside of a request context: no Effect context is available to run queries",
    );
  }
  return context;
};

export * as AuthRequestContext from "./AuthRequestContext.ts";
