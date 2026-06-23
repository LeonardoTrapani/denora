import { assert, describe, it } from "@effect/vitest";
import { eq } from "drizzle-orm";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Result from "effect/Result";
import { AgentRunPersistence } from "../../src/agent-run/AgentRunPersistence.ts";
import { makeInMemoryEventStreamStore } from "../../src/agent-run/EventStreamStore.ts";
import { Db } from "../../src/persistence/Db.ts";
import {
  agentRuns,
  denoraEventStreamEntries,
  denoraSessions,
} from "../../src/persistence/schema.ts";
import * as Database from "../helpers/Database.ts";

const persistenceLayer = AgentRunPersistence.layer.pipe(Layer.provideMerge(Database.dbLayer));

describe("AgentRunPersistence", () => {
  it.effect("registers runs with private input and owner authorization", () =>
    Effect.gen(function* () {
      const persistence = yield* AgentRunPersistence.Service;
      const runId = `run_${crypto.randomUUID()}`;
      const input = { prompt: "hello" };

      const registered = yield* persistence.registerRun({
        runId,
        userId: "user_1",
        input,
      });

      assert.isTrue(registered.created);
      assert.strictEqual(registered.runId, runId);
      assert.deepStrictEqual(yield* persistence.getRunInput(runId), input);
      yield* persistence.authorizeRun({ runId, userId: "user_1" });

      const denied = yield* persistence
        .authorizeRun({ runId, userId: "user_2" })
        .pipe(Effect.result);
      assert.isTrue(Result.isFailure(denied));
      if (Result.isFailure(denied)) assert.strictEqual(denied.failure._tag, "RunNotAuthorized");
    }).pipe(Effect.provide(persistenceLayer)),
  );

  it.effect("mirrors public stream events into Denora persistence tables", () =>
    Effect.gen(function* () {
      const persistence = yield* AgentRunPersistence.Service;
      const db = yield* Db.Service;
      const runId = `run_${crypto.randomUUID()}`;
      const path = `runs/${runId}`;
      const store = AgentRunPersistence.mirrorEventStreamStore(
        makeInMemoryEventStreamStore(),
        persistence,
      );

      yield* persistence.registerRun({ runId, userId: "user_1", input: { prompt: "hello" } });
      yield* store.createStream(path);
      yield* store.appendEvent(path, {
        v: 3,
        type: "run_start",
        runId,
        eventIndex: 0,
        timestamp: "2026-01-01T00:00:00.000Z",
        workflowName: "denora.agent-run",
        startedAt: "2026-01-01T00:00:00.000Z",
        input: { prompt: "hello" },
      });
      yield* store.appendEvent(path, {
        v: 3,
        type: "agent_end",
        runId,
        eventIndex: 1,
        timestamp: "2026-01-01T00:00:01.000Z",
        messages: [{ role: "assistant", content: [{ type: "text", text: "hello" }] }],
      });
      yield* store.appendEvent(path, {
        v: 3,
        type: "run_end",
        runId,
        eventIndex: 2,
        timestamp: "2026-01-01T00:00:02.000Z",
        isError: false,
        durationMs: 1000,
        result: { assistantText: "hello", messageCount: 1 },
      });
      yield* store.closeStream(path);

      const entries = yield* db.client
        .select()
        .from(denoraEventStreamEntries)
        .where(eq(denoraEventStreamEntries.path, path));
      const sessions = yield* db.client
        .select()
        .from(denoraSessions)
        .where(eq(denoraSessions.id, runId));
      const runs = yield* db.client.select().from(agentRuns).where(eq(agentRuns.id, runId));

      assert.lengthOf(entries, 3);
      assert.lengthOf(sessions, 1);
      assert.strictEqual(runs[0]?.status, "completed");
      assert.deepStrictEqual(runs[0]?.result, { assistantText: "hello", messageCount: 1 });
    }).pipe(Effect.provide(persistenceLayer)),
  );
});
