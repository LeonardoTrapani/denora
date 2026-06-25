import { assert, describe, it } from "@effect/vitest";
import { eq } from "drizzle-orm";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { ConversationPersistence } from "../../src/conversation/ConversationPersistence.ts";
import { Db } from "../../src/persistence/Db.ts";
import { agentRuns, denoraRuns } from "../../src/persistence/schema.ts";
import * as Database from "../helpers/Database.ts";

const persistenceLayer = ConversationPersistence.layer.pipe(Layer.provideMerge(Database.dbLayer));

describe("ConversationPersistence", () => {
  it.effect("archives and deletes conversations through semantic lifecycle methods", () =>
    Effect.gen(function* () {
      const persistence = yield* ConversationPersistence.Service;
      const conversationId = `conversation_${crypto.randomUUID()}`;

      yield* persistence.createConversation({ conversationId, userId: "user_1" });
      const archived = yield* persistence.archiveConversation({ conversationId, userId: "user_1" });
      const archivedAgain = yield* persistence.archiveConversation({
        conversationId,
        userId: "user_1",
      });
      const deleted = yield* persistence.deleteConversation({ conversationId, userId: "user_1" });
      const archiveAfterDelete = yield* persistence.archiveConversation({
        conversationId,
        userId: "user_1",
      });
      const deleteAgain = yield* persistence.deleteConversation({
        conversationId,
        userId: "user_1",
      });

      assert.strictEqual(archived.status, "archived");
      assert.isString(archived.archivedAt);
      assert.strictEqual(archivedAgain.status, "archived");
      assert.strictEqual(archivedAgain.archivedAt, archived.archivedAt);
      assert.strictEqual(deleted.status, "deleted");
      assert.strictEqual(archiveAfterDelete.status, "deleted");
      assert.strictEqual(deleteAgain.status, "deleted");
    }).pipe(Effect.provide(persistenceLayer)),
  );

  it.effect("rejects new submissions after a conversation is archived", () =>
    Effect.gen(function* () {
      const persistence = yield* ConversationPersistence.Service;
      const conversationId = `conversation_${crypto.randomUUID()}`;

      yield* persistence.createConversation({ conversationId, userId: "user_1" });
      const archived = yield* persistence.archiveConversation({ conversationId, userId: "user_1" });

      const error = yield* persistence
        .submitMessage({ conversationId, userId: "user_1", message: "hello" })
        .pipe(Effect.flip);

      assert.strictEqual(archived.status, "archived");
      assert.strictEqual(error._tag, "ConversationNotActive");
      if (error._tag !== "ConversationNotActive") return;
      assert.strictEqual(error.status, "archived");
    }).pipe(Effect.provide(persistenceLayer)),
  );

  it.effect("rejects new submissions after a conversation starts deleting", () =>
    Effect.gen(function* () {
      const persistence = yield* ConversationPersistence.Service;
      const conversationId = `conversation_${crypto.randomUUID()}`;

      yield* persistence.createConversation({ conversationId, userId: "user_1" });
      const deleting = yield* persistence.setConversationLifecycle({
        conversationId,
        userId: "user_1",
        status: "deleting",
      });

      const error = yield* persistence
        .submitMessage({ conversationId, userId: "user_1", message: "hello" })
        .pipe(Effect.flip);

      assert.strictEqual(deleting.status, "deleting");
      assert.strictEqual(error._tag, "ConversationNotActive");
      if (error._tag !== "ConversationNotActive") return;
      assert.strictEqual(error.status, "deleting");
    }).pipe(Effect.provide(persistenceLayer)),
  );

  it.effect(
    "does not append assistant output when a run finishes into a deleted conversation",
    () =>
      Effect.gen(function* () {
        const persistence = yield* ConversationPersistence.Service;
        const db = yield* Db.Service;
        const conversationId = `conversation_${crypto.randomUUID()}`;
        const submitted = yield* persistence.submitMessage({
          conversationId,
          userId: "user_1",
          message: "hello",
        });
        yield* persistence.recordSubmissionStarted({
          conversationId,
          userId: "user_1",
          agentName: "default",
          messageId: submitted.messageId,
          submissionId: submitted.submissionId,
          runId: submitted.runId,
          content: (submitted.input as { readonly submittedMessage: unknown }).submittedMessage,
        });

        yield* persistence.setConversationLifecycle({
          conversationId,
          userId: "user_1",
          status: "deleted",
        });
        yield* persistence.finishRun({
          runId: submitted.runId,
          isError: false,
          durationMs: 10,
          result: { assistantText: "must not persist" },
        });

        const messages = yield* persistence.listMessages({ conversationId, userId: "user_1" });
        assert.deepEqual(
          messages.map((message) => message.role),
          ["user"],
        );

        const [agentRun] = yield* db.client
          .select({ status: agentRuns.status, result: agentRuns.result, error: agentRuns.error })
          .from(agentRuns)
          .where(eq(agentRuns.id, submitted.runId));
        const [denoraRun] = yield* db.client
          .select({ result: denoraRuns.result, error: denoraRuns.error })
          .from(denoraRuns)
          .where(eq(denoraRuns.runId, submitted.runId));

        assert.strictEqual(agentRun?.status, "cancelled");
        assert.strictEqual(agentRun?.result, null);
        assert.deepEqual(agentRun?.error, {
          message: `Conversation ${conversationId} is deleted; run output was discarded.`,
        });
        assert.strictEqual(denoraRun?.result, null);
        assert.deepEqual(JSON.parse(denoraRun?.error ?? "null"), {
          message: `Conversation ${conversationId} is deleted; run output was discarded.`,
        });
      }).pipe(Effect.provide(persistenceLayer)),
  );

  it.effect("admits attached submissions and assembles history when processing starts", () =>
    Effect.gen(function* () {
      const persistence = yield* ConversationPersistence.Service;
      const conversationId = `conversation_${crypto.randomUUID()}`;

      const conversation = yield* persistence.createConversation({
        conversationId,
        userId: "user_1",
        title: "Inbox",
      });
      assert.strictEqual(conversation.id, conversationId);
      assert.strictEqual(conversation.ownerUserId, "user_1");

      const first = yield* persistence.submitMessage({
        conversationId,
        userId: "user_1",
        message: "hello",
      });
      assert.strictEqual(first.conversationId, conversationId);
      assert.match(first.submissionId, /^submission_/);
      assert.match(first.runId, /^run_/);
      assert.strictEqual(first.streamPath, `agents/default/${conversationId}`);
      assert.deepInclude(first.input as Record<string, unknown>, {
        userId: "user_1",
        submittedMessage: { text: "hello" },
      });

      const second = yield* persistence.submitMessage({
        conversationId,
        userId: "user_1",
        message: "what did I say?",
      });

      const firstStarted = yield* persistence.recordSubmissionStarted({
        conversationId,
        userId: "user_1",
        agentName: "default",
        messageId: first.messageId,
        submissionId: first.submissionId,
        runId: first.runId,
        content: (first.input as { readonly submittedMessage: unknown }).submittedMessage,
      });
      assert.deepInclude(firstStarted.input as Record<string, unknown>, { prompt: "hello" });

      yield* persistence.markRunStarted(first.runId);
      yield* persistence.finishRun({
        runId: first.runId,
        isError: false,
        durationMs: 10,
        result: { assistantText: "hi there", messageCount: 2 },
      });

      const secondStarted = yield* persistence.recordSubmissionStarted({
        conversationId,
        userId: "user_1",
        agentName: "default",
        messageId: second.messageId,
        submissionId: second.submissionId,
        runId: second.runId,
        content: (second.input as { readonly submittedMessage: unknown }).submittedMessage,
      });
      const secondInput = secondStarted.input as { readonly messages?: ReadonlyArray<unknown> };
      assert.isAtLeast(secondInput.messages?.length ?? 0, 2);

      const messages = yield* persistence.listMessages({ conversationId, userId: "user_1" });
      assert.deepEqual(
        messages.map((message) => message.role),
        ["user", "assistant", "user"],
      );
    }).pipe(Effect.provide(persistenceLayer)),
  );
});
