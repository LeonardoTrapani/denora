import * as HttpApiEndpoint from "effect/unstable/httpapi/HttpApiEndpoint";
import * as HttpApiGroup from "effect/unstable/httpapi/HttpApiGroup";
import * as HttpApiSchema from "effect/unstable/httpapi/HttpApiSchema";
import { Authorization } from "../../auth/Authorization.ts";
import { Agent, AgentHandleTaken, AgentList, CreateAgentPayload } from "./Schema.ts";

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
  .prefix("/agents")
  .middleware(Authorization.Service) {}
