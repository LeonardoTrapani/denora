import * as Schema from "effect/Schema";
import * as HttpApi from "effect/unstable/httpapi/HttpApi";
import * as HttpApiEndpoint from "effect/unstable/httpapi/HttpApiEndpoint";
import * as HttpApiGroup from "effect/unstable/httpapi/HttpApiGroup";
import * as HttpApiSchema from "effect/unstable/httpapi/HttpApiSchema";

export class Health extends Schema.Class<Health>("Health")({
  status: Schema.Literal("ok"),
}) {}

export class Agent extends Schema.Class<Agent>("Agent")({
  id: Schema.String,
  userId: Schema.String,
  name: Schema.String,
  handle: Schema.String,
  createdAt: Schema.String,
}) {}

export class AgentList extends Schema.Class<AgentList>("AgentList")({
  agents: Schema.Array(Agent),
}) {}

export class CreateAgentPayload extends Schema.Class<CreateAgentPayload>("CreateAgentPayload")({
  name: Schema.String,
  handle: Schema.String,
}) {}

export class AgentHandleTaken extends Schema.TaggedErrorClass<AgentHandleTaken>()(
  "AgentHandleTaken",
  { handle: Schema.String },
) {}

export class SystemGroup extends HttpApiGroup.make("System", { topLevel: true }).add(
  HttpApiEndpoint.get("health", "/health", {
    success: Health,
  }),
) {}

export class AgentsGroup extends HttpApiGroup.make("Agents")
  .add(
    HttpApiEndpoint.get("listAgents", "/", {
      success: AgentList,
    }),
    HttpApiEndpoint.post("createAgent", "/", {
      payload: CreateAgentPayload,
      success: Agent,
      error: AgentHandleTaken.pipe(HttpApiSchema.status(409)),
    }),
  )
  .prefix("/agents") {}

export class DenoraApi extends HttpApi.make("DenoraApi").add(SystemGroup).add(AgentsGroup) {}

export * as Api from "./Api.ts";
