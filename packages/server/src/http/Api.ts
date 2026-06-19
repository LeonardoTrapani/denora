import * as HttpApi from "effect/unstable/httpapi/HttpApi";
import { AgentGroup } from "./agent/Api.ts";
import { AccountGroup } from "./account/Api.ts";
import { SystemGroup } from "./system/Api.ts";

export { AccountGroup } from "./account/Api.ts";
export { AgentGroup } from "./agent/Api.ts";
export { AgentMessageResponse, SendMessageRequest } from "../agent/Schema.ts";
export { Health } from "./system/Schema.ts";
export { SystemGroup } from "./system/Api.ts";

export class DenoraApi extends HttpApi.make("DenoraApi")
  .add(SystemGroup)
  .add(AccountGroup)
  .add(AgentGroup) {}

export * as Api from "./Api.ts";
