import * as HttpApiEndpoint from "effect/unstable/httpapi/HttpApiEndpoint";
import * as HttpApiGroup from "effect/unstable/httpapi/HttpApiGroup";
import { DenoraUser, Unauthorized } from "../../auth/User.ts";

export class AccountPublicGroup extends HttpApiGroup.make("Account", { topLevel: true }).add(
  HttpApiEndpoint.get("me", "/me", {
    error: Unauthorized,
    success: DenoraUser,
  }),
) {}
