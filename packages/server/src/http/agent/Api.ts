import * as HttpApiEndpoint from "effect/unstable/httpapi/HttpApiEndpoint";
import * as HttpApiGroup from "effect/unstable/httpapi/HttpApiGroup";
import * as Schema from "effect/Schema";
import { AgentMessageResponse, AgentThreadError, SendMessageRequest } from "../../agent/Schema.ts";
import { AuthorizationApi } from "../../auth/AuthorizationApi.ts";

export class AgentGroup extends HttpApiGroup.make("Agent")
  .add(
    HttpApiEndpoint.post("sendAgentMessage", "/:agentId/threads/:threadId/messages", {
      params: {
        agentId: Schema.String,
        threadId: Schema.String,
      },
      payload: SendMessageRequest,
      success: AgentMessageResponse,
      error: AgentThreadError,
    }),
  )
  .prefix("/agents")
  .middleware(AuthorizationApi.Service) {}

export * as AgentApi from "./Api.ts";
