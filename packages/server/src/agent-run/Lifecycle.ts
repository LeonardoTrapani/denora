import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import {
  type EventStreamError,
  type EventStreamStore,
  parseOffset,
  runStreamPath,
} from "./EventStreamStore.ts";
import { AgentRunSession, type RunEvent } from "./AgentRunSession.ts";
import { isBufferedRunEvent, isStreamExcludedRunEvent } from "./RunEventContract.ts";
import type { Interface as PiRuntimeInterface } from "../agent-loop/PiRuntime.ts";

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

export interface ExecuteScheduledRunInput {
  readonly runId: string;
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
  const offset = yield* store.appendEvent(streamPath, {
    v: 1,
    type: "run_start",
    runId: input.runId,
    workflowName: "denora.agent-run",
    startedAt: timestamp,
    timestamp,
    eventIndex: 0,
  });

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

export const executeScheduledRun = Effect.fn("AgentRunLifecycle.executeScheduledRun")(function* (
  store: EventStreamStore,
  input: ExecuteScheduledRunInput,
): Effect.fn.Return<void, EventStreamError> {
  const streamPath = runStreamPath(input.runId);
  const meta = yield* store.getStreamMeta(streamPath);
  if (meta === null || meta.closed) return;

  // TODO(agent-run-storage): recover the private run input from a RunStore/RunRegistry
  // record instead of the public durable stream. Flue stores input outside the
  // public run event stream; Denora needs the same split before DO alarm recovery
  // can resume input-bearing runs safely.
  yield* executeRun(store, { runId: input.runId, input: undefined, pi: input.pi });
});

export const executeRun = Effect.fn("AgentRunLifecycle.executeRun")(function* (
  store: EventStreamStore,
  input: ExecuteRunInput,
): Effect.fn.Return<void, EventStreamError> {
  const streamPath = runStreamPath(input.runId);
  const meta = yield* store.getStreamMeta(streamPath);
  if (meta === null || meta.closed) return;

  let eventIndex = (yield* parseOffset(meta.nextOffset)) + 1;
  let bufferedEvents: RunEvent[] = [];

  const appendDecoratedRunEvent = Effect.fn("AgentRunLifecycle.appendDecoratedRunEvent")(function* (
    event: RunEvent,
  ): Effect.fn.Return<void, EventStreamError> {
    const timestamp = DateTime.formatIso(yield* DateTime.now);
    yield* store.appendEvent(streamPath, {
      ...event,
      runId: input.runId,
      v: 1,
      eventIndex: eventIndex++,
      timestamp,
    });
  });

  const flushBufferedRunEvents = Effect.fn("AgentRunLifecycle.flushBufferedRunEvents")(
    function* (): Effect.fn.Return<void, EventStreamError> {
      if (bufferedEvents.length === 0) return;
      const batch = bufferedEvents;
      bufferedEvents = [];
      for (const event of batch) {
        yield* appendDecoratedRunEvent(event);
      }
    },
  );

  const appendRunEvent = Effect.fn("AgentRunLifecycle.appendRunEvent")(function* (
    event: RunEvent,
  ): Effect.fn.Return<void, EventStreamError> {
    if (isStreamExcludedRunEvent(event)) return;
    if (isBufferedRunEvent(event)) {
      bufferedEvents.push(event);
      return;
    }
    yield* flushBufferedRunEvents();
    yield* appendDecoratedRunEvent(event);
  });

  const closeStream = store
    .closeStream(streamPath)
    .pipe(Effect.catch((error) => Effect.logError("agent run stream close failed", { error })));

  yield* AgentRunSession.execute({
    runId: input.runId,
    input: input.input,
    streamFn: input.pi.streamFn,
    onAgentEvent: appendRunEvent,
  }).pipe(
    Effect.matchEffect({
      onFailure: (error) =>
        flushBufferedRunEvents().pipe(
          Effect.flatMap(() =>
            appendFailedRunEnd(store, streamPath, input.runId, eventIndex++, error.message),
          ),
        ),
      onSuccess: (result) =>
        Effect.gen(function* () {
          yield* flushBufferedRunEvents();
          const timestamp = DateTime.formatIso(yield* DateTime.now);
          yield* store.appendEvent(streamPath, {
            v: 1,
            type: "run_end",
            runId: input.runId,
            eventIndex: eventIndex++,
            timestamp,
            outcome: "completed",
            result: {
              assistantText: result.assistantText,
              messageCount: result.messages.length,
            },
          });
        }),
    }),
    Effect.onError(() =>
      flushBufferedRunEvents().pipe(
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
  message: string,
): Effect.fn.Return<void, EventStreamError> {
  const timestamp = DateTime.formatIso(yield* DateTime.now);
  yield* store.appendEvent(streamPath, {
    v: 1,
    type: "run_end",
    runId,
    eventIndex,
    timestamp,
    outcome: "failed",
    error: { message },
  });
});

export * as AgentRunLifecycle from "./Lifecycle.ts";
