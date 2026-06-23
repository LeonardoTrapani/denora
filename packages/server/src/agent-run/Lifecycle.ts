import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import {
  type EventStreamError,
  type EventStreamStore,
  parseOffset,
  runStreamPath,
} from "./EventStreamStore.ts";
import { AgentRunSession, type RunEvent } from "./AgentRunSession.ts";
import {
  isBufferedRunEvent,
  isStreamExcludedRunEvent,
  redactRunEventImages,
} from "./RunEventContract.ts";
import type { Interface as PiRuntimeInterface } from "../agent-loop/PiRuntime.ts";

const BUFFERED_EVENT_FLUSH_INTERVAL_MS = 3_000;

export interface CreateRunInput {
  readonly runId: string;
  readonly input?: unknown;
  readonly userId?: string | undefined;
}

export interface CreateRunResult {
  readonly runId: string;
  readonly streamPath: string;
  readonly offset: string;
  readonly created: boolean;
}

export interface StartRunInput extends CreateRunInput {
  readonly scheduleExecution: (runId: string) => Effect.Effect<void, EventStreamError>;
}

export interface ExecuteRunInput extends CreateRunInput {
  readonly pi: PiRuntimeInterface;
}

export const createRun = Effect.fn("AgentRunLifecycle.createRun")(function* (
  store: EventStreamStore,
  input: CreateRunInput,
): Effect.fn.Return<CreateRunResult, EventStreamError> {
  const streamPath = runStreamPath(input.runId);
  const existing = yield* store.getStreamMeta(streamPath);
  if (existing !== null) {
    // Temporary Flue-shaped idempotency until Denora has a real Run registry.
    // Flue backs this with first-writer-wins RunStore records and route-time
    // workflow middleware checks. Here we only know that the stream exists, not
    // which user/agent/thread owns it. Reference: ~/.local/share/opencode/repos/
    // github.com/withastro/flue/packages/runtime/src/runtime/run-store.ts.
    return { runId: input.runId, streamPath, offset: existing.nextOffset, created: false };
  }

  yield* store.createStream(streamPath);
  const timestamp = DateTime.formatIso(yield* DateTime.now);
  const offset = yield* store.appendEvent(
    streamPath,
    redactRunEventImages({
      v: 3,
      type: "run_start",
      runId: input.runId,
      workflowName: "denora.agent-run",
      startedAt: timestamp,
      input: input.input,
      timestamp,
      eventIndex: 0,
    }),
  );

  return { runId: input.runId, streamPath, offset, created: true };
});

export const startRun = Effect.fn("AgentRunLifecycle.startRun")(function* (
  store: EventStreamStore,
  input: StartRunInput,
): Effect.fn.Return<CreateRunResult, EventStreamError> {
  const created = yield* createRun(store, input);
  if (created.created) {
    yield* input.scheduleExecution(input.runId).pipe(
      Effect.onError(() =>
        store.closeStream(created.streamPath).pipe(
          Effect.catch((error) =>
            Effect.logError("agent run stream close after scheduling failure failed", {
              error,
            }),
          ),
        ),
      ),
    );
  }
  return created;
});

export const executeRun = Effect.fn("AgentRunLifecycle.executeRun")(function* (
  store: EventStreamStore,
  input: ExecuteRunInput,
): Effect.fn.Return<void, EventStreamError> {
  const streamPath = runStreamPath(input.runId);
  const meta = yield* store.getStreamMeta(streamPath);
  if (meta === null || meta.closed) return;

  let eventIndex = (yield* parseOffset(meta.nextOffset)) + 1;
  const startedAtMs = Date.now();
  const subscribers = new Set<(event: RunEvent) => Effect.Effect<void, EventStreamError>>();

  const decorateRunEvent = Effect.fn("AgentRunLifecycle.decorateRunEvent")(function* (
    event: RunEvent,
  ): Effect.fn.Return<RunEvent> {
    const timestamp = DateTime.formatIso(yield* DateTime.now);
    return {
      ...event,
      runId: input.runId,
      v: 3,
      eventIndex: eventIndex++,
      timestamp,
    };
  });

  const emitRunEvent = Effect.fn("AgentRunLifecycle.emitRunEvent")(function* (
    event: RunEvent,
  ): Effect.fn.Return<void, EventStreamError> {
    if (isStreamExcludedRunEvent(event)) return;
    const decorated = yield* decorateRunEvent(event);
    for (const subscriber of subscribers) yield* subscriber(decorated);
  });

  const fanout = subscribeRunFanout(store, streamPath, subscribers);

  const closeStream = store
    .closeStream(streamPath)
    .pipe(Effect.catch((error) => Effect.logError("agent run stream close failed", { error })));

  yield* AgentRunSession.execute({
    runId: input.runId,
    input: input.input,
    streamFn: input.pi.streamFn,
    onAgentEvent: emitRunEvent,
  }).pipe(
    Effect.matchEffect({
      onFailure: (error) =>
        fanout
          .flush()
          .pipe(
            Effect.flatMap(() =>
              appendFailedRunEnd(
                store,
                streamPath,
                input.runId,
                eventIndex++,
                startedAtMs,
                error.message,
              ),
            ),
          ),
      onSuccess: (result) =>
        Effect.gen(function* () {
          yield* fanout.flush();
          const timestamp = DateTime.formatIso(yield* DateTime.now);
          yield* store.appendEvent(streamPath, {
            v: 3,
            type: "run_end",
            runId: input.runId,
            eventIndex: eventIndex++,
            timestamp,
            isError: false,
            durationMs: Math.max(0, Date.now() - startedAtMs),
            result: {
              assistantText: result.assistantText,
              messageCount: result.messages.length,
            },
          });
        }),
    }),
    Effect.onError(() =>
      fanout.flush().pipe(
        Effect.catch((error) =>
          Effect.logError("agent run buffered event flush before terminal failure failed", {
            error,
          }),
        ),
        Effect.flatMap(() =>
          appendFailedRunEnd(
            store,
            streamPath,
            input.runId,
            eventIndex++,
            startedAtMs,
            "Agent run execution failed.",
          ),
        ),
        Effect.catch((error) =>
          Effect.logError("agent run terminal failure event append failed", { error }),
        ),
      ),
    ),
    Effect.ensuring(closeStream),
  );
});

const appendFailedRunEnd = Effect.fn("AgentRunLifecycle.appendFailedRunEnd")(function* (
  store: EventStreamStore,
  streamPath: string,
  runId: string,
  eventIndex: number,
  startedAtMs: number,
  message: string,
): Effect.fn.Return<void, EventStreamError> {
  const timestamp = DateTime.formatIso(yield* DateTime.now);
  yield* store.appendEvent(streamPath, {
    v: 3,
    type: "run_end",
    runId,
    eventIndex,
    timestamp,
    isError: true,
    result: null,
    durationMs: Math.max(0, Date.now() - startedAtMs),
    error: { message },
  });
});

const subscribeRunFanout = (
  store: EventStreamStore,
  streamPath: string,
  subscribers: Set<(event: RunEvent) => Effect.Effect<void, EventStreamError>>,
): { readonly flush: () => Effect.Effect<void, EventStreamError> } => {
  let bufferedEvents: RunEvent[] = [];
  let bufferTimer: ReturnType<typeof setTimeout> | undefined;
  let timerFlush: Promise<void> | undefined;
  let timerFailure: EventStreamError | undefined;
  let active = true;

  const clearBufferTimer = () => {
    if (bufferTimer === undefined) return;
    clearTimeout(bufferTimer);
    bufferTimer = undefined;
  };

  const flushBufferedEventsNow = Effect.fn("AgentRunLifecycle.flushBufferedEventsNow")(
    function* (): Effect.fn.Return<void, EventStreamError> {
      clearBufferTimer();
      if (bufferedEvents.length === 0) return;
      const batch = bufferedEvents;
      bufferedEvents = [];
      for (const event of batch) yield* store.appendEvent(streamPath, event).pipe(Effect.asVoid);
    },
  );

  const scheduleBufferFlush = () => {
    if (bufferTimer !== undefined || !active) return;
    bufferTimer = setTimeout(() => {
      bufferTimer = undefined;
      timerFlush = Effect.runPromise(flushBufferedEventsNow())
        .catch((error: unknown) => {
          if (isEventStreamError(error)) timerFailure = error;
          console.error("[denora:event-stream] buffered appendEvent failed:", error);
        })
        .finally(() => {
          timerFlush = undefined;
          if (active && bufferedEvents.length > 0 && timerFailure === undefined)
            scheduleBufferFlush();
        });
    }, BUFFERED_EVENT_FLUSH_INTERVAL_MS);
  };

  const appendEvent = Effect.fn("AgentRunLifecycle.fanoutAppendEvent")(function* (
    event: RunEvent,
  ): Effect.fn.Return<void, EventStreamError> {
    if (isBufferedRunEvent(event)) {
      bufferedEvents.push(event);
      scheduleBufferFlush();
      return;
    }
    yield* flushBufferedEvents(false);
    yield* store.appendEvent(streamPath, event).pipe(Effect.asVoid);
  });

  const flushBufferedEvents = Effect.fn("AgentRunLifecycle.flushBufferedEvents")(function* (
    final: boolean,
  ): Effect.fn.Return<void, EventStreamError> {
    if (final) active = false;
    clearBufferTimer();
    const inFlight = timerFlush;
    if (inFlight !== undefined) yield* Effect.promise(() => inFlight);
    if (timerFailure !== undefined) return yield* timerFailure;
    yield* flushBufferedEventsNow();
  });

  subscribers.add(appendEvent);

  return {
    flush: () =>
      flushBufferedEvents(true).pipe(
        Effect.ensuring(Effect.sync(() => subscribers.delete(appendEvent))),
      ),
  };
};

const isEventStreamError = (error: unknown): error is EventStreamError =>
  typeof error === "object" &&
  error !== null &&
  "_tag" in error &&
  typeof (error as { readonly _tag?: unknown })._tag === "string";

export * as AgentRunLifecycle from "./Lifecycle.ts";
