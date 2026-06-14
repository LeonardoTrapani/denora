import * as ClientApi from "@denora/server/client-api";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Atom from "effect/unstable/reactivity/Atom";
import * as MobileApi from "./Api.ts";
import * as MobileAuth from "./Auth.ts";

export const Reset: typeof Atom.Reset = Atom.Reset;

export const emptyRuntime = Atom.runtime(Layer.empty);

export const runtime = Atom.runtime(MobileApi.layer);

export const health = runtime.atom(
  Effect.gen(function* () {
    const client = yield* ClientApi.DenoraClient;
    return yield* client.health();
  }),
);

export const loadAccount = runtime.fn<void>()(
  Effect.fn("MobileAtoms.loadAccount")(function* () {
    const client = yield* ClientApi.DenoraClient;
    return yield* client.me();
  }),
);

export const signIn: Atom.AtomResultFn<void, MobileAuth.AuthRedirectResult | null> =
  Atom.fn<void>()(
    Effect.fn("MobileAtoms.signIn")(function* () {
      return yield* Effect.promise(() => MobileAuth.signIn());
    }),
  );

export const signOut: Atom.AtomResultFn<void, void> = Atom.fn<void>()(
  Effect.fn("MobileAtoms.signOut")(function* () {
    return yield* Effect.promise(() => MobileAuth.signOutLocally());
  }),
);
