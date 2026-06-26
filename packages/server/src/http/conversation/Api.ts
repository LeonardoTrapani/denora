import * as Schema from "effect/Schema";
import * as HttpApiEndpoint from "effect/unstable/httpapi/HttpApiEndpoint";
import * as HttpApiGroup from "effect/unstable/httpapi/HttpApiGroup";
import * as HttpApiSchema from "effect/unstable/httpapi/HttpApiSchema";
import { AuthorizationApi } from "../../auth/AuthorizationApi.ts";
import { ConversationDomain } from "../../conversation/ConversationDomain.ts";
import { ConversationRequestFailed } from "./Errors.ts";

export const CreateConversationPayload = Schema.Struct({
  conversationId: Schema.optional(ConversationDomain.ConversationId),
  agentId: Schema.optional(Schema.NullOr(Schema.String)),
  title: Schema.optional(Schema.NullOr(Schema.String)),
  metadata: Schema.optional(Schema.Unknown),
}).pipe(Schema.annotate({ identifier: "CreateConversationPayload" }));
export type CreateConversationPayload = typeof CreateConversationPayload.Type;

export class Conversation extends Schema.Class<Conversation>("Conversation")({
  id: ConversationDomain.ConversationId,
  ownerUserId: ConversationDomain.UserId,
  agentId: Schema.NullOr(Schema.String),
  status: Schema.Literals(["active", "archiving", "archived", "deleting", "deleted"]),
  title: Schema.NullOr(Schema.String),
  metadata: Schema.Unknown,
  createdAt: Schema.String,
  updatedAt: Schema.String,
  archivedAt: Schema.NullOr(Schema.String),
}) {}

export class ConversationMessage extends Schema.Class<ConversationMessage>("ConversationMessage")({
  id: ConversationDomain.MessageId,
  conversationId: ConversationDomain.ConversationId,
  runId: Schema.NullOr(ConversationDomain.RunId),
  role: Schema.Literals(["system", "user", "assistant", "tool", "event"]),
  content: Schema.Unknown,
  metadata: Schema.Unknown,
  createdAt: Schema.String,
}) {}

export const SubmitConversationMessagePayload = Schema.Struct({
  message: Schema.optional(Schema.String),
  content: Schema.optional(Schema.Unknown),
}).pipe(Schema.annotate({ identifier: "SubmitConversationMessagePayload" }));
export type SubmitConversationMessagePayload = typeof SubmitConversationMessagePayload.Type;

export class SubmitConversationMessageResponse extends Schema.Class<SubmitConversationMessageResponse>(
  "SubmitConversationMessageResponse",
)({
  conversationId: ConversationDomain.ConversationId,
  messageId: ConversationDomain.MessageId,
  submissionId: ConversationDomain.SubmissionId,
  runId: ConversationDomain.RunId,
  streamUrl: Schema.String,
  streamPath: Schema.String,
  offset: Schema.String,
}) {}

export const AbortConversationPayload = Schema.Struct({
  reason: Schema.optional(Schema.String),
}).pipe(Schema.annotate({ identifier: "AbortConversationPayload" }));
export type AbortConversationPayload = typeof AbortConversationPayload.Type;

export class AbortConversationResponse extends Schema.Class<AbortConversationResponse>(
  "AbortConversationResponse",
)({
  abortedSubmissions: Schema.Number,
}) {}

const ConversationParams = { conversationId: Schema.String };

export class ConversationGroup extends HttpApiGroup.make("Conversation", { topLevel: true })
  .add(
    HttpApiEndpoint.post("createConversation", "/conversations", {
      payload: CreateConversationPayload,
      success: Conversation,
      error: ConversationRequestFailed,
    }),
  )
  .add(
    HttpApiEndpoint.get("listConversations", "/conversations", {
      success: Schema.Array(Conversation),
      error: ConversationRequestFailed,
    }),
  )
  .add(
    HttpApiEndpoint.get("listConversationMessages", "/conversations/:conversationId/messages", {
      params: ConversationParams,
      success: Schema.Array(ConversationMessage),
      error: ConversationRequestFailed,
    }),
  )
  .add(
    HttpApiEndpoint.post("submitConversationMessage", "/conversations/:conversationId/messages", {
      params: ConversationParams,
      payload: SubmitConversationMessagePayload,
      success: SubmitConversationMessageResponse.pipe(HttpApiSchema.status("Accepted")),
      error: ConversationRequestFailed,
    }),
  )
  .add(
    HttpApiEndpoint.post("abortConversation", "/conversations/:conversationId/abort", {
      params: ConversationParams,
      payload: AbortConversationPayload,
      success: AbortConversationResponse,
      error: ConversationRequestFailed,
    }),
  )
  .add(
    HttpApiEndpoint.post("archiveConversation", "/conversations/:conversationId/archive", {
      params: ConversationParams,
      success: Conversation,
      error: ConversationRequestFailed,
    }),
  )
  .add(
    HttpApiEndpoint.post("deleteConversation", "/conversations/:conversationId/delete", {
      params: ConversationParams,
      success: Conversation,
      error: ConversationRequestFailed,
    }),
  )
  .middleware(AuthorizationApi.Service) {}

export * as ConversationApi from "./Api.ts";
