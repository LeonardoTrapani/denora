import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import {
  type EventStreamError,
  type EventStreamStore,
  EventStorageFailed,
  agentStreamPath,
  parseOffset,
  runStreamPath,
} from "./EventStreamStore.ts";
import {
  AgentRunSession,
  type AssistantStreamEventCallback,
  type RunCheckpoint,
  type RunEvent,
} from "./AgentRunSession.ts";
import {
  isBufferedRunEvent,
  isStreamExcludedRunEvent,
  redactRunEventImages,
  type LlmUserMessage,
} from "./RunEventContract.ts";
import type { Interface as PiRuntimeInterface } from "../agent-loop/PiRuntime.ts";
import { ConversationDomain } from "../conversation/ConversationDomain.ts";

const BUFFERED_EVENT_FLUSH_INTERVAL_MS = 3_000;

export interface CreateRunInput {
  readonly runId: string;
  readonly conversationId?: string | undefined;
  readonly submissionId?: string | undefined;
  readonly triggerMessageId?: string | undefined;
  readonly input?: unknown;
  readonly userId?: string | undefined;
}

export interface CreateConversationSubmissionInput {
  readonly runId: string;
  readonly agentName: string;
  readonly conversationId: string;
  readonly submissionId: string;
  readonly triggerMessageId: string;
  readonly parentMessageId?: string | undefined;
  readonly input?: unknown;
  readonly userId?: string | undefined;
  readonly waitForResult?: boolean | undefined;
}

export interface CreateRunResult {
  readonly runId: string;
  readonly streamPath: string;
  readonly offset: string;
  readonly created: boolean;
}

export interface CreateConversationSubmissionResult extends CreateRunResult {
  readonly conversationId: string;
  readonly submissionId: string;
  readonly messageId: string;
}

export interface AppendConversationUserMessageAppliedInput {
  readonly agentName: string;
  readonly conversationId: string;
  readonly submissionId: string;
  readonly userTurnId: string;
  readonly message: LlmUserMessage;
}

export interface AppendConversationUserMessageAppliedResult {
  readonly startOffset: string;
  readonly endOffset: string;
}

export interface StartRunInput extends CreateRunInput {
  readonly scheduleExecution: (runId: string) => Effect.Effect<void, EventStreamError>;
}

export interface ExecuteRunInput extends CreateRunInput {
  readonly pi: PiRuntimeInterface;
  readonly signal?: AbortSignal | undefined;
}

export interface ExecuteRunAttemptInput extends ExecuteRunInput {}
export interface ExecuteConversationSubmissionAttemptInput extends CreateConversationSubmissionInput {
  readonly pi: PiRuntimeInterface;
  readonly beforeEmitEvent?:
    | ((event: RunEvent) => Effect.Effect<void, EventStreamError>)
    | undefined;
  readonly onCheckpoint?:
    | ((checkpoint: RunCheckpoint) => Effect.Effect<void, EventStreamError>)
    | undefined;
  readonly onAssistantStreamEvent?: AssistantStreamEventCallback | undefined;
  readonly initialAssistantMessageIndex?: number | undefined;
  readonly signal?: AbortSignal | undefined;
}

export interface ExecuteRunAttemptResult {
  readonly terminalEvent: RunEvent;
  readonly durationMs: number;
  readonly isError: boolean;
  readonly result?: unknown;
  readonly error?: { readonly message: string } | undefined;
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
    // which user/agent/thread owns it. Reference:
    // vendor/flue/packages/runtime/src/runtime/run-store.ts.
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

export const createConversationSubmission = Effect.fn(
  "AgentRunLifecycle.createConversationSubmission",
)(function* (
  store: EventStreamStore,
  input: CreateConversationSubmissionInput,
): Effect.fn.Return<CreateConversationSubmissionResult, EventStreamError> {
  const streamPath = agentStreamPath(input.agentName, input.conversationId);
  const existing = yield* store.getStreamMeta(streamPath);
  const offset = existing?.nextOffset ?? "-1";
  yield* store.createStream(streamPath);

  return {
    runId: input.runId,
    conversationId: input.conversationId,
    submissionId: input.submissionId,
    messageId: input.triggerMessageId,
    streamPath,
    offset,
    created: existing === null,
  };
});

export const appendConversationUserMessageApplied = Effect.fn(
  "AgentRunLifecycle.appendConversationUserMessageApplied",
)(function* (
  store: EventStreamStore,
  input: AppendConversationUserMessageAppliedInput,
): Effect.fn.Return<AppendConversationUserMessageAppliedResult, EventStreamError> {
  const streamPath = agentStreamPath(input.agentName, input.conversationId);
  yield* store.createStream(streamPath);
  const meta = yield* store.getStreamMeta(streamPath);
  if (meta === null) {
    return yield* new EventStorageFailed({
      operation: "append applied conversation user message",
      cause: new Error(`Conversation stream ${streamPath} was not created.`),
    });
  }

  const startKey = `submission:${input.submissionId}:user:start`;
  const endKey = `submission:${input.submissionId}:user:end`;
  const existingStart = yield* store.readEventByKey(streamPath, startKey);
  const existingEnd = yield* store.readEventByKey(streamPath, endKey);
  const existingStartIndex = eventIndexFrom(existingStart?.event);
  const existingEndIndex = eventIndexFrom(existingEnd?.event);
  const firstIndex =
    existingStartIndex ??
    (existingEndIndex === undefined
      ? (yield* parseOffset(meta.nextOffset)) + 1
      : existingEndIndex - 1);
  const timestamp =
    timestampFrom(existingStart?.event) ??
    timestampFrom(existingEnd?.event) ??
    DateTime.formatIso(yield* DateTime.now);

  const startOffset = yield* store.appendEventOnce(
    streamPath,
    startKey,
    redactRunEventImages({
      v: 3,
      type: "message_start",
      instanceId: input.conversationId,
      conversationId: input.conversationId,
      agentName: input.agentName,
      submissionId: input.submissionId,
      turnId: input.userTurnId,
      eventIndex: firstIndex,
      timestamp,
      message: input.message,
    }),
  );
  const endOffset = yield* store.appendEventOnce(
    streamPath,
    endKey,
    redactRunEventImages({
      v: 3,
      type: "message_end",
      instanceId: input.conversationId,
      conversationId: input.conversationId,
      agentName: input.agentName,
      submissionId: input.submissionId,
      turnId: input.userTurnId,
      eventIndex: firstIndex + 1,
      timestamp,
      message: input.message,
    }),
  );

  return { startOffset, endOffset };
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
  const result = yield* executeRunAttempt(store, input);
  const streamPath = runStreamPath(input.runId);
  yield* store.appendEvent(streamPath, result.terminalEvent).pipe(Effect.asVoid);
  yield* store
    .closeStream(streamPath)
    .pipe(Effect.catch((error) => Effect.logError("agent run stream close failed", { error })));
});

export const executeRunAttempt = Effect.fn("AgentRunLifecycle.executeRunAttempt")(function* (
  store: EventStreamStore,
  input: ExecuteRunAttemptInput,
): Effect.fn.Return<ExecuteRunAttemptResult, EventStreamError> {
  const streamPath = runStreamPath(input.runId);
  const meta = yield* store.getStreamMeta(streamPath);
  if (meta === null)
    return yield* new EventStorageFailed({
      operation: "execute agent run attempt",
      cause: new Error(`Agent run stream ${streamPath} does not exist.`),
    });
  if (meta.closed)
    return yield* new EventStorageFailed({
      operation: "execute agent run attempt",
      cause: new Error(`Agent run stream ${streamPath} is closed.`),
    });

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

  return yield* AgentRunSession.execute({
    runId: input.runId,
    input: input.input,
    streamFn: input.pi.streamFn,
    tools: input.pi.tools,
    onAgentEvent: emitRunEvent,
    signal: input.signal,
  }).pipe(
    Effect.matchEffect({
      onFailure: (error) =>
        fanout
          .flush()
          .pipe(
            Effect.flatMap(() =>
              makeFailedRunEnd(input.runId, eventIndex++, startedAtMs, error.message),
            ),
          ),
      onSuccess: (result) =>
        Effect.gen(function* () {
          yield* fanout.flush();
          const timestamp = DateTime.formatIso(yield* DateTime.now);
          const terminalEvent = {
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
          } satisfies RunEvent;
          return {
            terminalEvent,
            durationMs: terminalEvent.durationMs as number,
            isError: false,
            result: terminalEvent.result,
          } satisfies ExecuteRunAttemptResult;
        }),
    }),
    Effect.onError(() =>
      fanout.flush().pipe(
        Effect.catch((error) =>
          Effect.logError("agent run buffered event flush before terminal failure failed", {
            error,
          }),
        ),
      ),
    ),
  );
});

export const executeConversationSubmissionAttempt = Effect.fn(
  "AgentRunLifecycle.executeConversationSubmissionAttempt",
)(function* (
  store: EventStreamStore,
  input: ExecuteConversationSubmissionAttemptInput,
): Effect.fn.Return<ExecuteRunAttemptResult, EventStreamError> {
  const streamPath = agentStreamPath(input.agentName, input.conversationId);
  const meta = yield* store.getStreamMeta(streamPath);
  if (meta === null)
    return yield* new EventStorageFailed({
      operation: "execute agent conversation submission",
      cause: new Error(`Agent conversation stream ${streamPath} does not exist.`),
    });
  if (meta.closed)
    return yield* new EventStorageFailed({
      operation: "execute agent conversation submission",
      cause: new Error(`Agent conversation stream ${streamPath} is closed.`),
    });

  let eventIndex = (yield* parseOffset(meta.nextOffset)) + 1;
  const startedAtMs = Date.now();
  const subscribers = new Set<(event: RunEvent) => Effect.Effect<void, EventStreamError>>();

  const decorateConversationEvent = Effect.fn("AgentRunLifecycle.decorateConversationEvent")(
    function* (event: RunEvent): Effect.fn.Return<RunEvent> {
      const timestamp = DateTime.formatIso(yield* DateTime.now);
      const { runId: _runId, ...attachedEvent } = event;
      return redactRunEventImages({
        ...attachedEvent,
        instanceId: input.conversationId,
        conversationId: input.conversationId,
        agentName: input.agentName,
        submissionId: input.submissionId,
        v: 3,
        eventIndex: eventIndex++,
        timestamp,
      });
    },
  );

  const emitConversationEvent = Effect.fn("AgentRunLifecycle.emitConversationEvent")(function* (
    event: RunEvent,
  ): Effect.fn.Return<void, EventStreamError> {
    if (isStreamExcludedRunEvent(event)) return;
    if (input.beforeEmitEvent !== undefined) yield* input.beforeEmitEvent(event);
    const decorated = yield* decorateConversationEvent(event);
    for (const subscriber of subscribers) yield* subscriber(decorated);
  });

  const fanout = subscribeRunFanout(store, streamPath, subscribers);

  return yield* AgentRunSession.execute({
    runId: input.runId,
    input: input.input,
    streamFn: input.pi.streamFn,
    tools: input.pi.tools,
    onAgentEvent: emitConversationEvent,
    onCheckpoint: input.onCheckpoint,
    onAssistantStreamEvent: input.onAssistantStreamEvent,
    initialAssistantMessageIndex: input.initialAssistantMessageIndex,
    signal: input.signal,
  }).pipe(
    Effect.matchEffect({
      onFailure: (error) =>
        fanout
          .flush()
          .pipe(
            Effect.flatMap(() =>
              makeFailedSubmissionSettled(input, eventIndex++, startedAtMs, error.message),
            ),
          ),
      onSuccess: (result) =>
        Effect.gen(function* () {
          yield* fanout.flush();
          const timestamp = DateTime.formatIso(yield* DateTime.now);
          const terminalEvent = {
            v: 3,
            type: "submission_settled",
            instanceId: input.conversationId,
            conversationId: input.conversationId,
            agentName: input.agentName,
            submissionId: input.submissionId,
            eventIndex: eventIndex++,
            timestamp,
            outcome: "completed",
            result: {
              assistantText: result.assistantText,
              messageCount: result.messages.length,
            },
          } satisfies RunEvent;
          return {
            terminalEvent,
            durationMs: Math.max(0, Date.now() - startedAtMs),
            isError: false,
            result: terminalEvent.result,
          } satisfies ExecuteRunAttemptResult;
        }),
    }),
    Effect.onError(() =>
      fanout.flush().pipe(
        Effect.catch((error) =>
          Effect.logError(
            "agent conversation buffered event flush before terminal failure failed",
            {
              error,
            },
          ),
        ),
      ),
    ),
  );
});

const makeFailedRunEnd = Effect.fn("AgentRunLifecycle.makeFailedRunEnd")(function* (
  runId: string,
  eventIndex: number,
  startedAtMs: number,
  message: string,
): Effect.fn.Return<ExecuteRunAttemptResult> {
  const timestamp = DateTime.formatIso(yield* DateTime.now);
  const terminalEvent = {
    v: 3,
    type: "run_end",
    runId,
    eventIndex,
    timestamp,
    isError: true,
    result: null,
    durationMs: Math.max(0, Date.now() - startedAtMs),
    error: { message },
  } satisfies RunEvent;
  return {
    terminalEvent,
    durationMs: terminalEvent.durationMs as number,
    isError: true,
    result: null,
    error: { message },
  } satisfies ExecuteRunAttemptResult;
});

const makeFailedSubmissionSettled = Effect.fn("AgentRunLifecycle.makeFailedSubmissionSettled")(
  function* (
    input: CreateConversationSubmissionInput,
    eventIndex: number,
    startedAtMs: number,
    message: string,
  ): Effect.fn.Return<ExecuteRunAttemptResult> {
    const timestamp = DateTime.formatIso(yield* DateTime.now);
    const terminalEvent = {
      v: 3,
      type: "submission_settled",
      instanceId: input.conversationId,
      conversationId: input.conversationId,
      agentName: input.agentName,
      submissionId: input.submissionId,
      eventIndex,
      timestamp,
      outcome: "failed",
      result: null,
      error: { message },
    } satisfies RunEvent;
    return {
      terminalEvent,
      durationMs: Math.max(0, Date.now() - startedAtMs),
      isError: true,
      result: null,
      error: { message },
    } satisfies ExecuteRunAttemptResult;
  },
);

const eventIndexFrom = ConversationDomain.eventIndexFrom;

const timestampFrom = ConversationDomain.timestampFrom;

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
