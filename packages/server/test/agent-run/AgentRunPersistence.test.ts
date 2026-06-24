import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Result from "effect/Result";
import { AgentRunPersistence } from "../../src/agent-run/AgentRunPersistence.ts";
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
      const streamAuthorization = yield* persistence.authorizeRunForStream({
        runId,
        userId: "user_1",
      });
      assert.strictEqual(streamAuthorization.runId, runId);
      assert.strictEqual(streamAuthorization.conversationId, registered.conversationId);
      assert.strictEqual(streamAuthorization.streamPath, registered.streamPath);

      const denied = yield* persistence
        .authorizeRun({ runId, userId: "user_2" })
        .pipe(Effect.result);
      assert.isTrue(Result.isFailure(denied));
      if (Result.isFailure(denied)) assert.strictEqual(denied.failure._tag, "RunNotAuthorized");
    }).pipe(Effect.provide(persistenceLayer)),
  );
});
