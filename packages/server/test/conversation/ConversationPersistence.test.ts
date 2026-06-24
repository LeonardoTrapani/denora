import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { ConversationPersistence } from "../../src/conversation/ConversationPersistence.ts";
import * as Database from "../helpers/Database.ts";

const persistenceLayer = ConversationPersistence.layer.pipe(Layer.provideMerge(Database.dbLayer));

describe("ConversationPersistence", () => {
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
