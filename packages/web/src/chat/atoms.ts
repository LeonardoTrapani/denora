import * as ClientApi from "@denora/server/client-api";
import * as Effect from "effect/Effect";
import type { Effect as EffectType } from "effect/Effect";
import * as Atom from "effect/unstable/reactivity/Atom";

import { clientLayer, type DenoraApiClient } from "../lib/api.ts";

type EffectSuccess<T> = T extends EffectType<infer A, infer _E, infer _R> ? A : never;

export type ConversationSummary = EffectSuccess<
  ReturnType<DenoraApiClient["listConversations"]>
>[number];

const apiAtoms = Atom.runtime(clientLayer);

export const loadConversationsAtom = apiAtoms.fn<void>()(
  Effect.fn("ChatAtoms.loadConversations")(function* () {
    const client = yield* ClientApi.DenoraClient;
    return yield* client.listConversations();
  }),
);

export * as ChatAtoms from "./atoms.ts";
