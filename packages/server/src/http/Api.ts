import * as HttpApi from "effect/unstable/httpapi/HttpApi";
import { AgentRunGroup } from "./agent-run/Api.ts";
import { AccountGroup } from "./account/Api.ts";
import { SystemGroup } from "./system/Api.ts";

export { AccountGroup } from "./account/Api.ts";
export { AgentRunGroup } from "./agent-run/Api.ts";
export { Health } from "./system/Schema.ts";
export { SystemGroup } from "./system/Api.ts";

export class DenoraApi extends HttpApi.make("DenoraApi")
  .add(SystemGroup)
  .add(AccountGroup)
  .add(AgentRunGroup) {}

export * as Api from "./Api.ts";
