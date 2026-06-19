import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai-compat";
import type { RuntimeContext } from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import { AiError, LanguageModel, type Response as AiResponse } from "effect/unstable/ai";
import { FetchHttpClient } from "effect/unstable/http";
import { HttpServerRequest } from "effect/unstable/http/HttpServerRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import { AgentAiGateway } from "../../../src/agent/AiGateway.ts";
import { MODEL, MODEL_CONFIG } from "../../../src/agent/ThreadObject.ts";

export default class OpenAiGatewayWorker extends Cloudflare.Worker<OpenAiGatewayWorker>()(
  "OpenAiGatewayWorker",
  {
    main: import.meta.filename,
    compatibility: {
      flags: ["nodejs_compat"],
    },
  },
  Effect.gen(function* () {
    const aiGateway = yield* Cloudflare.AiGateway.bind(AgentAiGateway.Gateway);
    const languageModel = Layer.unwrap(
      Effect.gen(function* () {
        const apiUrl = yield* aiGateway.getUrl("openai");
        return OpenAiLanguageModel.layer({ model: MODEL, config: MODEL_CONFIG }).pipe(
          Layer.provide(OpenAiClient.layer({ apiUrl }).pipe(Layer.provide(FetchHttpClient.layer))),
        );
      }),
    );

    return {
      fetch: Effect.gen(function* () {
        const request = yield* HttpServerRequest;
        const ctx = yield* Effect.context<RuntimeContext>();
        const url = new URL(request.url, "http://worker");
        const prompt =
          url.searchParams.get("prompt") ?? "Reply with the single word pong and no punctuation.";

        if (url.pathname === "/gateway-url") {
          const providerUrl = yield* aiGateway.getUrl("openai").pipe(Effect.orDie);
          const gatewayId = yield* aiGateway.id;
          return yield* HttpServerResponse.json({
            gatewayId,
            provider: "openai",
            providerUrl,
          });
        }

        if (url.pathname === "/generate") {
          return yield* LanguageModel.generateText({ prompt }).pipe(
            Effect.provide(languageModel),
            Effect.flatMap((response) =>
              HttpServerResponse.json({
                ok: true,
                text: response.text,
                finishReason: response.finishReason,
                usage: {
                  inputTokens: response.usage.inputTokens.total,
                  outputTokens: response.usage.outputTokens.total,
                },
              }),
            ),
            Effect.catch((error) => gatewayFailureResponse(error)),
          );
        }

        if (url.pathname === "/stream") {
          const body = LanguageModel.streamText({ prompt }).pipe(
            Stream.provide(languageModel),
            Stream.provideContext(ctx),
            Stream.filter((part): part is AiResponse.TextDeltaPart => part.type === "text-delta"),
            Stream.map((part) => part.delta),
            Stream.filter((chunk) => chunk.length > 0),
            Stream.encodeText,
          );

          return HttpServerResponse.stream(body, {
            headers: {
              "cache-control": "no-cache",
              "content-type": "text/plain; charset=utf-8",
              "x-accel-buffering": "no",
            },
          });
        }

        return HttpServerResponse.text("not found", { status: 404 });
      }),
    };
  }).pipe(Effect.provide(Cloudflare.AiGatewayBindingLive)),
) {}

const gatewayFailureResponse = (error: unknown) =>
  HttpServerResponse.json(
    {
      ok: false,
      message: gatewayFailureMessage(error),
    },
    { status: 502 },
  );

const gatewayFailureMessage = (error: unknown): string => {
  if (AiError.isAiError(error)) {
    if (error.reason._tag === "AuthenticationError" && error.reason.kind === "InvalidKey") {
      return "Cloudflare AI Gateway OpenAI authentication failed. Ensure the gateway has OpenAI access through a provider config or forwarded provider key.";
    }

    return `OpenAI via Cloudflare AI Gateway failed: ${error.reason._tag}`;
  }

  if (error instanceof Error && error.message.length > 0) return error.message;
  return "OpenAI via Cloudflare AI Gateway failed";
};
