import * as Schema from "effect/Schema";
import { HandleTaken } from "../../agents/AgentRepository.ts";

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

export { HandleTaken as AgentHandleTaken };
