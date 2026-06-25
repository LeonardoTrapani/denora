import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { AgentConversationCoordinator } from "../../src/agent-run/AgentConversationCoordinator.ts";
import { AgentConversationSessionStore } from "../../src/agent-run/AgentConversationSessionStore.ts";
import { EventStreamStore as EventStreamStoreModule } from "../../src/agent-run/EventStreamStore.ts";
import { AgentRunLifecycle } from "../../src/agent-run/Lifecycle.ts";
import { SqlStorage } from "../../src/agent-run/SqlStorage.ts";
import { ConversationPersistence } from "../../src/conversation/ConversationPersistence.ts";
import { Conversations } from "../../src/conversation/Conversations.ts";
import * as Database from "../helpers/Database.ts";
import { SqliteStorage } from "../helpers/SqliteStorage.ts";

describe("Conversations", () => {
  it.effect(
    "archives a conversation through the public service and propagates to the coordinator",
    () =>
      Effect.gen(function* () {
        const conversations = yield* Conversations.Service;
        const coordinator = yield* AgentConversationCoordinator.Service;
        const store = yield* EventStreamStoreModule.Service;
        const conversationId = `conversation_${crypto.randomUUID()}`;
        const input = {
          agentName: "default",
          conversationId,
          input: { userId: "user_1", submittedMessage: { text: "queued before delete" } },
          runId: "run_queued_before_delete",
          submissionId: "submission_queued_before_delete",
          triggerMessageId: "message_queued_before_delete",
          userId: "user_1",
        };

        yield* conversations.createConversation({ conversationId, userId: "user_1" });
        yield* coordinator.admitSubmission(input);
        yield* AgentRunLifecycle.createConversationSubmission(store, input);

        const updated = yield* conversations.archiveConversation({
          conversationId,
          userId: "user_1",
        });
        const repeated = yield* conversations.archiveConversation({
          conversationId,
          userId: "user_1",
        });
        const terminal = yield* coordinator.getSubmissionTerminal(input.submissionId);
        const event = terminal?.event as {
          readonly outcome?: unknown;
          readonly error?: { readonly message?: string };
        };
        const rejected = yield* coordinator
          .admitSubmission({
            ...input,
            runId: "run_after_delete",
            submissionId: "submission_after_delete",
          })
          .pipe(Effect.flip);

        assert.strictEqual(updated.status, "archived");
        assert.strictEqual(repeated.status, "archived");
        assert.strictEqual(event.outcome, "failed");
        assert.strictEqual(
          event.error?.message,
          `Conversation ${conversationId} is archived; agent submissions are not accepted.`,
        );
        assert.strictEqual(rejected._tag, "EventStorageFailed");
        if (rejected._tag !== "EventStorageFailed") return;
        assert.strictEqual(rejected.operation, "admit agent conversation submission");
      }).pipe(Effect.provide(conversationsIntegrationLayer)),
  );

  it.effect(
    "deletes a conversation through the public service and propagates to the coordinator",
    () =>
      Effect.gen(function* () {
        const conversations = yield* Conversations.Service;
        const coordinator = yield* AgentConversationCoordinator.Service;
        const store = yield* EventStreamStoreModule.Service;
        const conversationId = `conversation_${crypto.randomUUID()}`;
        const input = {
          agentName: "default",
          conversationId,
          input: { userId: "user_1", submittedMessage: { text: "queued before delete" } },
          runId: "run_queued_before_delete",
          submissionId: "submission_queued_before_delete",
          triggerMessageId: "message_queued_before_delete",
          userId: "user_1",
        };

        yield* conversations.createConversation({ conversationId, userId: "user_1" });
        yield* coordinator.admitSubmission(input);
        yield* AgentRunLifecycle.createConversationSubmission(store, input);

        const updated = yield* conversations.deleteConversation({
          conversationId,
          userId: "user_1",
        });
        const repeated = yield* conversations.deleteConversation({
          conversationId,
          userId: "user_1",
        });
        const terminal = yield* coordinator.getSubmissionTerminal(input.submissionId);
        const event = terminal?.event as {
          readonly outcome?: unknown;
          readonly error?: { readonly message?: string };
        };
        const rejected = yield* coordinator
          .admitSubmission({
            ...input,
            runId: "run_after_delete",
            submissionId: "submission_after_delete",
          })
          .pipe(Effect.flip);

        assert.strictEqual(updated.status, "deleted");
        assert.strictEqual(repeated.status, "deleted");
        assert.strictEqual(event.outcome, "failed");
        assert.strictEqual(
          event.error?.message,
          `Conversation ${conversationId} is deleted; agent submissions are not accepted.`,
        );
        assert.strictEqual(rejected._tag, "EventStorageFailed");
      }).pipe(Effect.provide(conversationsIntegrationLayer)),
  );
});

const conversationsLayer = Layer.unwrap(
  Effect.gen(function* () {
    const coordinator = yield* AgentConversationCoordinator.Service;
    const store = yield* EventStreamStoreModule.Service;
    return Conversations.layer({
      getByName: () => ({
        abortConversation: (input) => coordinator.abortConversation(input),
        fetch: () => Effect.die(new Error("Fetch is not used by this test.")),
        setConversationLifecycle: (input) => coordinator.setConversationLifecycle(input),
        submitMessage: (input) =>
          coordinator
            .admitSubmission(input)
            .pipe(
              Effect.flatMap(() => AgentRunLifecycle.createConversationSubmission(store, input)),
            ),
      }),
    });
  }),
);

const sqlStorageLayer = Layer.effect(
  SqlStorage.Service,
  Effect.gen(function* () {
    const sqlite = yield* SqliteStorage.Service;
    return SqlStorage.Service.of(sqlite.sql);
  }),
);

const conversationsIntegrationLayer = conversationsLayer.pipe(
  Layer.provideMerge(AgentConversationCoordinator.sqliteLayer),
  Layer.provideMerge(EventStreamStoreModule.sqliteLayer),
  Layer.provideMerge(AgentConversationSessionStore.sqliteLayer),
  Layer.provideMerge(ConversationPersistence.layer),
  Layer.provideMerge(Database.dbLayer),
  Layer.provideMerge(sqlStorageLayer),
  Layer.provide(SqliteStorage.layer),
);
