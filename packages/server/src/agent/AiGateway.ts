import * as Cloudflare from "alchemy/Cloudflare";

export const Gateway = Cloudflare.AiGateway("AgentGateway", {
  authentication: true,
  collectLogs: true,
});

export * as AgentAiGateway from "./AiGateway.ts";
