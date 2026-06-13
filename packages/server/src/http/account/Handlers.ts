import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import { Authorization } from "../../auth/Authorization.ts";
import { DenoraApi } from "../Api.ts";

export const layer = HttpApiBuilder.group(DenoraApi, "Account", (handlers) =>
  handlers.handle("me", () => Authorization.CurrentUser),
);

export * as AccountHandlers from "./Handlers.ts";
