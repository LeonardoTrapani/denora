import * as HttpApiEndpoint from "effect/unstable/httpapi/HttpApiEndpoint";
import * as HttpApiGroup from "effect/unstable/httpapi/HttpApiGroup";
import { AuthorizationApi } from "../../auth/AuthorizationApi.ts";

export class AccountGroup extends HttpApiGroup.make("Account", { topLevel: true })
  .add(
    HttpApiEndpoint.get("me", "/me", {
      success: AuthorizationApi.DenoraUser,
    }),
  )
  .middleware(AuthorizationApi.Service) {}
