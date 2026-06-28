import * as Schema from "effect/Schema";
import * as HttpApiEndpoint from "effect/unstable/httpapi/HttpApiEndpoint";
import * as HttpApiGroup from "effect/unstable/httpapi/HttpApiGroup";
import { AuthorizationApi } from "../../auth/AuthorizationApi.ts";

export const DisplayProvider = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
}).pipe(Schema.annotate({ identifier: "AiModelDisplayProvider" }));
export type DisplayProvider = typeof DisplayProvider.Type;

export const AiModelCatalogItem = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
  displayProvider: DisplayProvider,
  family: Schema.String,
  default: Schema.Boolean,
  api: Schema.Literals(["anthropic-messages", "openai-responses", "openai-completions"]),
  capabilities: Schema.Struct({
    reasoning: Schema.Boolean,
    reasoningMode: Schema.optional(
      Schema.Literals(["anthropic-manual", "anthropic-adaptive", "openai", "openai-compatible"]),
    ),
    tools: Schema.Boolean,
  }),
  inputModalities: Schema.Array(Schema.Literals(["text", "image"])),
  outputModalities: Schema.Array(Schema.Literal("text")),
  contextWindow: Schema.Number,
  maxTokens: Schema.Number,
  cost: Schema.Struct({
    input: Schema.Number,
    output: Schema.Number,
    cacheRead: Schema.Number,
    cacheWrite: Schema.Number,
  }),
  lifecycle: Schema.Literals(["stable", "preview", "deprecated"]),
}).pipe(Schema.annotate({ identifier: "AiModelCatalogItem" }));
export type AiModelCatalogItem = typeof AiModelCatalogItem.Type;

export const AiModelProviderGroup = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
  models: Schema.Array(AiModelCatalogItem),
}).pipe(Schema.annotate({ identifier: "AiModelProviderGroup" }));
export type AiModelProviderGroup = typeof AiModelProviderGroup.Type;

export const AiModelsResponse = Schema.Struct({
  defaultModelId: Schema.String,
  providers: Schema.Array(AiModelProviderGroup),
}).pipe(Schema.annotate({ identifier: "AiModelsResponse" }));
export type AiModelsResponse = typeof AiModelsResponse.Type;

export class AiGroup extends HttpApiGroup.make("Ai", { topLevel: true })
  .add(
    HttpApiEndpoint.get("listAiModels", "/ai/models", {
      success: AiModelsResponse,
    }),
  )
  .middleware(AuthorizationApi.Service) {}

export * as AiApi from "./Api.ts";
