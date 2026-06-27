import { FetchError, type LiveMode } from "@durable-streams/client";
import * as ClientApi from "@denora/server/client-api";
import { Cause, Effect, Exit, Schedule, Stream } from "effect";
import type { Effect as EffectType } from "effect/Effect";
import * as Atom from "effect/unstable/reactivity/Atom";

import { clientLayer, type DenoraApiClient } from "../lib/api.ts";
import {
  type ChatReducerEvent,
  type ChatState,
  type PersistedConversationMessage,
  emptyChatState,
  reduceChatEvent,
} from "./reducer.ts";
import { createConversationEventStream } from "./stream.ts";
import type { DenoraConversationEvent } from "./types.ts";

type EffectSuccess<T> = T extends EffectType<infer A, infer _E, infer _R> ? A : never;

export type ConversationSummary = EffectSuccess<
  ReturnType<DenoraApiClient["listConversations"]>
>[number];

export type ChatHistory = number | "all";

export interface ConversationStreamStart {
  readonly conversationId: string | undefined;
  readonly history?: ChatHistory | undefined;
  readonly live?: LiveMode | undefined;
  readonly hydrate?: boolean | undefined;
  readonly initialMessages?: ReadonlyArray<PersistedConversationMessage> | undefined;
}

export type ConversationMessageSubmit = {
  readonly message: string;
} & (
  | {
      readonly target: "new";
    }
  | {
      readonly target: "conversation";
      readonly conversationId: string;
      readonly initialMessages?: ReadonlyArray<PersistedConversationMessage> | undefined;
    }
);

const apiAtoms = Atom.runtime(clientLayer);

export const selectedConversationIdAtom = Atom.make<string | undefined>(undefined);
export const composerTextAtom = Atom.make("");
export const chatStateAtom = Atom.make<ChatState>({ ...emptyChatState, historyReady: true });
export const streamCheckpointAtom = Atom.make<string | undefined>(undefined);
export const localMessageCounterAtom = Atom.make(0);

export const loadConversationsAtom = apiAtoms.fn<void>()(
  Effect.fn("ChatAtoms.loadConversations")(function* () {
    const client = yield* ClientApi.DenoraClient;
    return yield* client.listConversations();
  }),
);

export const startConversationStreamAtom = apiAtoms.fn<ConversationStreamStart>()(
  Effect.fn("ChatAtoms.startConversationStream")(function* (input, get) {
    const dispatch = (event: ChatReducerEvent | DenoraConversationEvent) =>
      Effect.sync(() =>
        get.registry.update(chatStateAtom, (state) => reduceChatEvent(state, event)),
      );
    const setState = (state: ChatState) =>
      Effect.sync(() => get.registry.set(chatStateAtom, state));

    if (input.conversationId === undefined) {
      yield* Effect.sync(() => get.registry.set(streamCheckpointAtom, undefined));
      yield* setState({ ...emptyChatState, historyReady: true });
      return;
    }

    const current = get.registry.get(chatStateAtom);
    const shouldHydrate = input.hydrate ?? true;

    const hasInitialMessages = input.initialMessages !== undefined;

    if (shouldHydrate || current.conversationId !== input.conversationId) {
      yield* Effect.sync(() => get.registry.set(streamCheckpointAtom, undefined));
      if (hasInitialMessages) {
        const loaded = reduceChatEvent(
          { ...emptyChatState, conversationId: input.conversationId },
          { type: "local_history_loaded", messages: input.initialMessages },
        );
        yield* setState(reduceChatEvent(loaded, { type: "local_history_ready" }));
      } else {
        yield* setState({
          ...emptyChatState,
          conversationId: input.conversationId,
          historyReady: false,
          status: "hydrating",
        });
      }
    }

    if (!hasInitialMessages && (shouldHydrate || !get.registry.get(chatStateAtom).historyReady)) {
      yield* hydrateConversation(
        get,
        input.conversationId,
        input.history ?? 100,
        dispatch,
        setState,
      );
    }

    yield* connectConversation(get, input.conversationId, input.live ?? true, dispatch);
  }),
);

export const submitConversationMessageAtom = apiAtoms.fn<ConversationMessageSubmit>()(
  Effect.fn("ChatAtoms.submitConversationMessage")(function* (input, get) {
    const message = input.message.trim();
    if (message.length === 0) return;

    const localNumber = get.registry.modify(localMessageCounterAtom, (value) => [
      value + 1,
      value + 1,
    ]);
    const localId = `local:${localNumber}`;
    const dispatch = (event: ChatReducerEvent | DenoraConversationEvent) =>
      Effect.sync(() =>
        get.registry.update(chatStateAtom, (state) => reduceChatEvent(state, event)),
      );

    yield* prepareConversationForSubmit(get, input);

    const conversationExit = yield* Effect.exit(ensureConversation(get, dispatch, input));
    if (Exit.isFailure(conversationExit)) {
      const error = toError(Cause.squash(conversationExit.cause));
      yield* dispatch({ type: "local_send_failed", localId, error });
      return yield* Effect.fail(error);
    }

    yield* dispatch({ type: "local_send_submitted", localId, message });

    const client = yield* ClientApi.DenoraClient;
    const receiptExit = yield* Effect.exit(
      client.submitConversationMessage({
        params: { conversationId: conversationExit.value },
        payload: { message },
      }),
    );
    if (Exit.isFailure(receiptExit)) {
      const error = toError(Cause.squash(receiptExit.cause));
      yield* dispatch({ type: "local_send_failed", localId, error });
      return yield* Effect.fail(error);
    }

    yield* Effect.sync(() => get.registry.set(streamCheckpointAtom, receiptExit.value.offset));
    yield* dispatch({
      type: "local_send_admitted",
      localId,
      submissionId: receiptExit.value.submissionId,
    });
    yield* Effect.sync(() => {
      get.registry.set(startConversationStreamAtom, {
        conversationId: conversationExit.value,
        hydrate: false,
      });
      get.registry.set(loadConversationsAtom, undefined);
    });

    return conversationExit.value;
  }),
);

const prepareConversationForSubmit = Effect.fn("ChatAtoms.prepareConversationForSubmit")(function* (
  get: Atom.FnContext,
  input: ConversationMessageSubmit,
) {
  yield* Effect.sync(() => {
    if (input.target === "new") {
      get.registry.set(streamCheckpointAtom, undefined);
      get.registry.set(chatStateAtom, { ...emptyChatState, historyReady: true });
      return;
    }

    const current = get.registry.get(chatStateAtom);
    if (current.conversationId === input.conversationId) return;

    get.registry.set(streamCheckpointAtom, undefined);
    if (input.initialMessages !== undefined) {
      const loaded = reduceChatEvent(
        { ...emptyChatState, conversationId: input.conversationId },
        { type: "local_history_loaded", messages: input.initialMessages },
      );
      get.registry.set(chatStateAtom, reduceChatEvent(loaded, { type: "local_history_ready" }));
      return;
    }

    get.registry.set(chatStateAtom, {
      ...emptyChatState,
      conversationId: input.conversationId,
      historyReady: false,
      status: "hydrating",
    });
  });
});

const ensureConversation = Effect.fn("ChatAtoms.ensureConversation")(function* (
  _get: Atom.FnContext,
  dispatch: (event: ChatReducerEvent) => Effect.Effect<void>,
  input: ConversationMessageSubmit,
) {
  if (input.target === "conversation") {
    return input.conversationId;
  }

  const client = yield* ClientApi.DenoraClient;
  const conversation = yield* client.createConversation({
    payload: { title: "New conversation" },
  });

  yield* dispatch({ type: "local_conversation_created", conversationId: conversation.id });
  yield* dispatch({ type: "local_history_ready" });
  return conversation.id;
});

const hydrateConversation = Effect.fn("ChatAtoms.hydrateConversation")(function* (
  get: Atom.FnContext,
  conversationId: string,
  history: ChatHistory,
  dispatch: (event: ChatReducerEvent) => Effect.Effect<void>,
  setState: (state: ChatState) => Effect.Effect<void>,
) {
  const retryStep = yield* Schedule.toStepWithSleep(reconnectSchedule);

  while (true) {
    yield* dispatch({ type: "local_status", status: "hydrating" });
    let hydrationState: ChatState = { ...emptyChatState, conversationId };
    let streamOffset = "-1";

    const streamExit = yield* Effect.exit(
      conversationEventStream(
        {
          conversationId,
          live: false,
          offset: "-1",
          ...(history === "all" ? {} : { tail: history }),
        },
        (offset) => {
          streamOffset = offset;
        },
      ).pipe(
        Stream.runForEach((event) =>
          Effect.sync(() => {
            hydrationState = reduceChatEvent(hydrationState, event);
          }),
        ),
      ),
    );

    if (Exit.isSuccess(streamExit)) {
      yield* Effect.sync(() => get.registry.set(streamCheckpointAtom, streamOffset));
      yield* setState(reduceChatEvent(hydrationState, { type: "local_history_ready" }));
      return;
    }

    const error = toError(Cause.squash(streamExit.cause));
    if (isStatus(error, 404)) {
      const client = yield* ClientApi.DenoraClient;
      const messages = yield* client.listConversationMessages({
        params: { conversationId },
      });
      const loaded = reduceChatEvent(
        { ...emptyChatState, conversationId },
        {
          type: "local_history_loaded",
          messages,
        },
      );
      yield* setState(reduceChatEvent(loaded, { type: "local_history_ready" }));
      return;
    }

    yield* dispatch({ type: "local_status", status: "hydrating", error });
    yield* retryStep(error);
  }
});

const connectConversation = Effect.fn("ChatAtoms.connectConversation")(function* (
  get: Atom.FnContext,
  conversationId: string,
  live: LiveMode,
  dispatch: (event: ChatReducerEvent | DenoraConversationEvent) => Effect.Effect<void>,
) {
  let reconnectOffset = get.registry.get(streamCheckpointAtom) ?? "-1";
  const retryStep = yield* Schedule.toStepWithSleep(reconnectSchedule);

  while (true) {
    yield* dispatch({ type: "local_status", status: "connecting" });
    let delivered = false;
    let latestOffset = reconnectOffset;

    const streamExit = yield* Effect.exit(
      conversationEventStream({ conversationId, offset: reconnectOffset, live }, (offset) => {
        latestOffset = offset;
      }).pipe(
        Stream.runForEach((event) =>
          Effect.sync(() => {
            delivered = true;
            get.registry.update(chatStateAtom, (state) => reduceChatEvent(state, event));
          }),
        ),
      ),
    );

    const error = Exit.isSuccess(streamExit)
      ? new Error("Conversation event stream ended unexpectedly")
      : toError(Cause.squash(streamExit.cause));
    if (delivered) reconnectOffset = latestOffset;

    if (!delivered && isStatus(error, 404)) {
      const checkpoint = get.registry.get(streamCheckpointAtom);
      if (checkpoint !== undefined) {
        reconnectOffset = checkpoint;
      } else {
        yield* dispatch({ type: "local_stream_missing" });
        return;
      }
    }

    yield* dispatch({ type: "local_status", status: "connecting", error });
    yield* retryStep(error);
  }
});

function conversationEventStream(
  options: Parameters<typeof createConversationEventStream>[0],
  onOffset?: ((offset: string) => void) | undefined,
) {
  return Stream.unwrap(
    Effect.acquireRelease(
      Effect.sync(() => createConversationEventStream(options)),
      (events) => Effect.sync(() => events.cancel()),
    ).pipe(
      Effect.map((events) =>
        Stream.fromAsyncIterable(events, toError).pipe(
          Stream.tap(() =>
            Effect.sync(() => {
              onOffset?.(events.offset);
            }),
          ),
        ),
      ),
    ),
  );
}

const reconnectSchedule = Schedule.exponential("1 seconds").pipe(
  Schedule.either(Schedule.spaced("30 seconds")),
  Schedule.setInputType<Error>(),
);

function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

function isStatus(error: unknown, status: number): boolean {
  return error instanceof FetchError && error.status === status;
}

export * as ChatAtoms from "./atoms.ts";
