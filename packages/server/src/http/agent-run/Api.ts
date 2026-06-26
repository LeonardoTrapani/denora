import * as Schema from "effect/Schema";
import * as HttpApiEndpoint from "effect/unstable/httpapi/HttpApiEndpoint";
import * as HttpApiGroup from "effect/unstable/httpapi/HttpApiGroup";
import { AuthorizationApi } from "../../auth/AuthorizationApi.ts";
import { ConversationDomain } from "../../conversation/ConversationDomain.ts";
import { CreateAgentRunFailed } from "./Errors.ts";

export const CreateAgentRunPayload = Schema.Struct({
  runId: Schema.optional(ConversationDomain.RunId),
  conversationId: Schema.optional(ConversationDomain.ConversationId),
  triggerMessageId: Schema.optional(ConversationDomain.MessageId),
  input: Schema.optional(Schema.Unknown),
}).pipe(Schema.annotate({ identifier: "CreateAgentRunPayload" }));
export type CreateAgentRunPayload = typeof CreateAgentRunPayload.Type;

export class CreateAgentRunResponse extends Schema.Class<CreateAgentRunResponse>(
  "CreateAgentRunResponse",
)({
  runId: ConversationDomain.RunId,
  streamUrl: Schema.String,
  streamPath: Schema.String,
  offset: Schema.String,
}) {}

export class AgentRunGroup extends HttpApiGroup.make("AgentRun", { topLevel: true })
  .add(
    HttpApiEndpoint.post("createAgentRun", "/agent-runs", {
      payload: CreateAgentRunPayload,
      success: CreateAgentRunResponse,
      error: CreateAgentRunFailed,
    }),
  )
  .middleware(AuthorizationApi.Service) {}

export * as AgentRunApi from "./Api.ts";
