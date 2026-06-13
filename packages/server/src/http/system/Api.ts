import * as HttpApiEndpoint from "effect/unstable/httpapi/HttpApiEndpoint";
import * as HttpApiGroup from "effect/unstable/httpapi/HttpApiGroup";
import { Health } from "./Schema.ts";

export class SystemGroup extends HttpApiGroup.make("System", { topLevel: true }).add(
  HttpApiEndpoint.get("health", "/health", {
    success: Health,
  }),
) {}
