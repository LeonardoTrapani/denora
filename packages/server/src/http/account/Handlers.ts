import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import { AuthorizationApi } from "../../auth/AuthorizationApi.ts";
import { DenoraApi } from "../Api.ts";

export const layer = HttpApiBuilder.group(DenoraApi, "Account", (handlers) =>
  handlers.handle("me", () => AuthorizationApi.CurrentUser),
);

export * as AccountHandlers from "./Handlers.ts";
