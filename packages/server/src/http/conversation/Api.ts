import * as Schema from "effect/Schema";
import * as HttpApiEndpoint from "effect/unstable/httpapi/HttpApiEndpoint";
import * as HttpApiGroup from "effect/unstable/httpapi/HttpApiGroup";
import { AuthorizationApi } from "../../auth/AuthorizationApi.ts";
import { ConversationRequestFailed } from "../../conversation/Conversations.ts";

export class CreateConversationPayload extends Schema.Class<CreateConversationPayload>(
  "CreateConversationPayload",
)({
  conversationId: Schema.optional(Schema.String),
  agentId: Schema.optional(Schema.NullOr(Schema.String)),
  title: Schema.optional(Schema.NullOr(Schema.String)),
  metadata: Schema.optional(Schema.Unknown),
}) {}

export class Conversation extends Schema.Class<Conversation>("Conversation")({
  id: Schema.String,
  ownerUserId: Schema.String,
  agentId: Schema.NullOr(Schema.String),
  status: Schema.Literals(["active", "archiving", "archived", "deleting", "deleted"]),
  title: Schema.NullOr(Schema.String),
  metadata: Schema.Unknown,
  createdAt: Schema.String,
  updatedAt: Schema.String,
  archivedAt: Schema.NullOr(Schema.String),
}) {}

export class ConversationMessage extends Schema.Class<ConversationMessage>("ConversationMessage")({
  id: Schema.String,
  conversationId: Schema.String,
  runId: Schema.NullOr(Schema.String),
  role: Schema.Literals(["system", "user", "assistant", "tool", "event"]),
  content: Schema.Unknown,
  metadata: Schema.Unknown,
  createdAt: Schema.String,
}) {}

export class SubmitConversationMessagePayload extends Schema.Class<SubmitConversationMessagePayload>(
  "SubmitConversationMessagePayload",
)({
  message: Schema.optional(Schema.String),
  content: Schema.optional(Schema.Unknown),
}) {}

export class SubmitConversationMessageResponse extends Schema.Class<SubmitConversationMessageResponse>(
  "SubmitConversationMessageResponse",
)({
  conversationId: Schema.String,
  messageId: Schema.String,
  submissionId: Schema.String,
  runId: Schema.String,
  streamUrl: Schema.String,
  streamPath: Schema.String,
  offset: Schema.String,
}) {}

export class AbortConversationPayload extends Schema.Class<AbortConversationPayload>(
  "AbortConversationPayload",
)({
  reason: Schema.optional(Schema.String),
}) {}

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
      success: SubmitConversationMessageResponse,
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
  .middleware(AuthorizationApi.Service) {}

export * as ConversationApi from "./Api.ts";
