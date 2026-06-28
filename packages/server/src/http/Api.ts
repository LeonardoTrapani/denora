import * as HttpApi from "effect/unstable/httpapi/HttpApi";
import { AccountGroup } from "./account/Api.ts";
import { AgentRunGroup } from "./agent-run/Api.ts";
import { AiGroup } from "./ai/Api.ts";
import { ConversationGroup } from "./conversation/Api.ts";
import { SystemGroup } from "./system/Api.ts";

export { AccountGroup } from "./account/Api.ts";
export { AgentRunGroup } from "./agent-run/Api.ts";
export { AiGroup, AiModelCatalogItem, AiModelProviderGroup, AiModelsResponse } from "./ai/Api.ts";
export {
  AbortConversationResponse,
  Conversation,
  ConversationGroup,
  ConversationMessage,
  SubmitConversationMessageResponse,
} from "./conversation/Api.ts";
export { Health } from "./system/Schema.ts";
export { SystemGroup } from "./system/Api.ts";

export class DenoraApi extends HttpApi.make("DenoraApi")
  .add(SystemGroup)
  .add(AccountGroup)
  .add(ConversationGroup)
  .add(AgentRunGroup)
  .add(AiGroup) {}

export * as Api from "./Api.ts";
