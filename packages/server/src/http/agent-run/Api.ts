import * as Schema from "effect/Schema";
import * as HttpApiEndpoint from "effect/unstable/httpapi/HttpApiEndpoint";
import * as HttpApiGroup from "effect/unstable/httpapi/HttpApiGroup";
import { AuthorizationApi } from "../../auth/AuthorizationApi.ts";
import { CreateAgentRunFailed } from "../../agent-run/AgentRuns.ts";

export class CreateAgentRunPayload extends Schema.Class<CreateAgentRunPayload>(
  "CreateAgentRunPayload",
)({
  runId: Schema.optional(Schema.String),
  input: Schema.optional(Schema.Unknown),
}) {}

export class CreateAgentRunResponse extends Schema.Class<CreateAgentRunResponse>(
  "CreateAgentRunResponse",
)({
  runId: Schema.String,
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
