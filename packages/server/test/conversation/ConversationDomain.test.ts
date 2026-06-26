import { assert, describe, it } from "@effect/vitest";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { ConversationDomain } from "../../src/conversation/ConversationDomain.ts";

describe("ConversationDomain branded ids", () => {
  it("factories return values that decode with their semantic and generated branded schemas", () => {
    const conversationId = ConversationDomain.makeConversationId();
    const messageId = ConversationDomain.makeMessageId();
    const submissionId = ConversationDomain.makeSubmissionId();
    const runId = ConversationDomain.makeRunId();

    assert.strictEqual(
      Schema.decodeSync(ConversationDomain.ConversationId)(conversationId),
      conversationId,
    );
    assert.strictEqual(
      Schema.decodeSync(ConversationDomain.GeneratedConversationId)(conversationId),
      conversationId,
    );
    assert.strictEqual(Schema.decodeSync(ConversationDomain.MessageId)(messageId), messageId);
    assert.strictEqual(
      Schema.decodeSync(ConversationDomain.GeneratedMessageId)(messageId),
      messageId,
    );
    assert.strictEqual(
      Schema.decodeSync(ConversationDomain.SubmissionId)(submissionId),
      submissionId,
    );
    assert.strictEqual(
      Schema.decodeSync(ConversationDomain.GeneratedSubmissionId)(submissionId),
      submissionId,
    );
    assert.strictEqual(Schema.decodeSync(ConversationDomain.RunId)(runId), runId);
    assert.strictEqual(Schema.decodeSync(ConversationDomain.GeneratedRunId)(runId), runId);
  });

  it("accepts non-empty legacy values with semantic id schemas", () => {
    assert.strictEqual(
      Schema.decodeSync(ConversationDomain.ConversationId)("550e8400-e29b-41d4-a716-446655440000"),
      "550e8400-e29b-41d4-a716-446655440000",
    );
    assert.strictEqual(
      Schema.decodeSync(ConversationDomain.MessageId)("legacy-message-id"),
      "legacy-message-id",
    );
    assert.strictEqual(
      Schema.decodeSync(ConversationDomain.SubmissionId)("legacy-submission-id"),
      "legacy-submission-id",
    );
    assert.strictEqual(
      Schema.decodeSync(ConversationDomain.RunId)("legacy-run-id"),
      "legacy-run-id",
    );
  });

  it("rejects empty values with semantic id schemas", () => {
    assert.isTrue(Option.isNone(Schema.decodeUnknownOption(ConversationDomain.ConversationId)("")));
    assert.isTrue(Option.isNone(Schema.decodeUnknownOption(ConversationDomain.MessageId)("")));
    assert.isTrue(Option.isNone(Schema.decodeUnknownOption(ConversationDomain.SubmissionId)("")));
    assert.isTrue(Option.isNone(Schema.decodeUnknownOption(ConversationDomain.RunId)("")));
  });

  it("rejects swapped prefixes with generated id schemas", () => {
    assert.isTrue(
      Option.isNone(
        Schema.decodeUnknownOption(ConversationDomain.GeneratedConversationId)("run_123"),
      ),
    );
    assert.isTrue(
      Option.isNone(
        Schema.decodeUnknownOption(ConversationDomain.GeneratedMessageId)("conversation_123"),
      ),
    );
    assert.isTrue(
      Option.isNone(
        Schema.decodeUnknownOption(ConversationDomain.GeneratedSubmissionId)("message_123"),
      ),
    );
    assert.isTrue(
      Option.isNone(
        Schema.decodeUnknownOption(ConversationDomain.GeneratedRunId)("submission_123"),
      ),
    );
  });

  it("rejects empty UserId and AgentName", () => {
    assert.isTrue(Option.isNone(Schema.decodeUnknownOption(ConversationDomain.UserId)("")));
    assert.isTrue(Option.isNone(Schema.decodeUnknownOption(ConversationDomain.AgentName)("")));
  });

  it("accepts valid UserId and AgentName", () => {
    assert.strictEqual(Schema.decodeSync(ConversationDomain.UserId)("user_123"), "user_123");
    assert.strictEqual(Schema.decodeSync(ConversationDomain.AgentName)("default"), "default");
  });
});
