import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import { type EventStreamError, type EventStreamStore, runStreamPath } from "./EventStreamStore.ts";

export interface CreateRunInput {
  readonly runId: string;
  readonly input?: unknown;
  readonly userId?: string | undefined;
}

export interface CreateRunResult {
  readonly runId: string;
  readonly streamPath: string;
  readonly offset: string;
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
    return { runId: input.runId, streamPath, offset: existing.nextOffset };
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
    input: {
      value: input.input ?? null,
      createdByUserId: input.userId,
    },
  });

  return { runId: input.runId, streamPath, offset };
});

export * as AgentRunLifecycle from "./Lifecycle.ts";
