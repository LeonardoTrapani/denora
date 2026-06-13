import * as HttpApi from "effect/unstable/httpapi/HttpApi";
import { AccountGroup } from "./account/Api.ts";
import { AgentsGroup } from "./agents/Api.ts";
import { SystemGroup } from "./system/Api.ts";

export { AccountGroup } from "./account/Api.ts";
export { AgentsGroup } from "./agents/Api.ts";
export { Agent, AgentHandleTaken, AgentList, CreateAgentPayload } from "./agents/Schema.ts";
export { Health } from "./system/Schema.ts";
export { SystemGroup } from "./system/Api.ts";

export class DenoraApi extends HttpApi.make("DenoraApi")
  .add(SystemGroup)
  .add(AccountGroup)
  .add(AgentsGroup) {}

export * as Api from "./Api.ts";
