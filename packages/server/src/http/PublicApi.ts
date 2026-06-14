import * as HttpApi from "effect/unstable/httpapi/HttpApi";
import { AccountPublicGroup } from "./account/PublicApi.ts";
import { SystemGroup } from "./system/Api.ts";

export { DenoraUser, Unauthorized } from "../auth/User.ts";
export { AccountPublicGroup } from "./account/PublicApi.ts";
export { Health } from "./system/Schema.ts";
export { SystemGroup } from "./system/Api.ts";

export class DenoraPublicApi extends HttpApi.make("DenoraApi")
  .add(SystemGroup)
  .add(AccountPublicGroup) {}
