import * as HttpApiEndpoint from "effect/unstable/httpapi/HttpApiEndpoint";
import * as HttpApiGroup from "effect/unstable/httpapi/HttpApiGroup";
import { Authorization } from "../../auth/Authorization.ts";

export class AccountGroup extends HttpApiGroup.make("Account", { topLevel: true })
  .add(
    HttpApiEndpoint.get("me", "/me", {
      success: Authorization.DenoraUser,
    }),
  )
  .middleware(Authorization.Service) {}
