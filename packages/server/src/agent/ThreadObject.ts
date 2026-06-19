import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai-compat";
import * as Cloudflare from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import { AiError, Chat, type Response } from "effect/unstable/ai";
import { FetchHttpClient } from "effect/unstable/http";
import { AgentAiGateway } from "./AiGateway.ts";
import { AgentThreadError } from "./Schema.ts";

export const MODEL = "gpt-4o-mini";
export const MODEL_CONFIG = { max_output_tokens: 1024, temperature: 0.3 };

export class ThreadObject extends Cloudflare.DurableObjectNamespace<ThreadObject>()(
  "AgentThreads",
  Cloudflare.AiGateway.bind(AgentAiGateway.Gateway).pipe(
    Effect.map((aiGateway) => {
      const languageModel = Layer.unwrap(
        Effect.gen(function* () {
          const apiUrl = yield* aiGateway.getUrl("openai");
          return OpenAiLanguageModel.layer({ model: MODEL, config: MODEL_CONFIG }).pipe(
            Layer.provide(
              OpenAiClient.layer({ apiUrl }).pipe(Layer.provide(FetchHttpClient.layer)),
            ),
          );
        }),
      );

      return Effect.gen(function* () {
        const persistence = yield* Chat.makePersisted({
          storeId: "denora.chat",
        }).pipe(Effect.provide(Cloudflare.DurableObjectChatPersistence));

        return {
          send: (threadId: string, prompt: string) =>
            Effect.gen(function* () {
              yield* Effect.logInfo("agent thread send start", {
                messageLength: prompt.length,
                threadId,
              });
              const chat = yield* persistence.getOrCreate(threadId);
              yield* Effect.logInfo("agent thread send chat loaded", {
                threadId,
              });
              const response = yield* chat.generateText({ prompt });
              const contentPartTypes = partTypes(response.content);
              yield* Effect.logInfo("agent thread send complete", {
                finishReason: response.finishReason,
                model: MODEL,
                partTypes: contentPartTypes,
                responseLength: response.text.length,
                threadId,
              });

              if (response.text.length === 0) {
                return yield* new AgentThreadError({
                  operation: "send",
                  message: "Assistant response completed without text",
                  model: MODEL,
                  detail: assistantResponseDetail({
                    finishReason: response.finishReason,
                    partTypes: contentPartTypes,
                    textLength: response.text.length,
                  }),
                });
              }

              return { content: response.text };
            }).pipe(
              Effect.provide(languageModel),
              Effect.catch((error) =>
                Effect.gen(function* () {
                  if (error instanceof AgentThreadError) {
                    yield* Effect.logError("agent thread send failed", {
                      threadId,
                      error,
                    });
                    return yield* error;
                  }
                  yield* Effect.logError("agent thread send failed", {
                    threadId,
                    error,
                  });
                  return yield* new AgentThreadError({
                    operation: "send",
                    message: agentThreadErrorMessage(
                      error,
                      "Unable to generate assistant response",
                    ),
                  });
                }),
              ),
            ),
          stream: (threadId: string, prompt: string) =>
            Stream.unwrap(
              Effect.gen(function* () {
                const chat = yield* persistence.getOrCreate(threadId);
                let emittedTextLength = 0;
                let finishReason = "unknown";
                const streamPartTypes = new Set<string>();

                yield* Effect.logInfo("agent thread stream chat loaded", {
                  model: MODEL,
                  messageLength: prompt.length,
                  threadId,
                });

                return chat.streamText({ prompt }).pipe(
                  Stream.provide(languageModel),
                  Stream.tap((part) =>
                    Effect.sync(() => {
                      streamPartTypes.add(part.type);
                      if (part.type === "finish") finishReason = part.reason;
                      if (part.type === "text-delta") emittedTextLength += part.delta.length;
                    }),
                  ),
                  Stream.filter(
                    (part): part is Response.TextDeltaPart => part.type === "text-delta",
                  ),
                  Stream.map((part) => part.delta),
                  Stream.filter((text) => text.length > 0),
                  Stream.orElseIfEmpty(() =>
                    Stream.fail(
                      new AgentThreadError({
                        operation: "stream",
                        message: "Assistant response completed without text",
                        model: MODEL,
                        detail: assistantResponseDetail({
                          finishReason,
                          partTypes: Array.from(streamPartTypes),
                          textLength: emittedTextLength,
                        }),
                      }),
                    ),
                  ),
                  Stream.tap((text) =>
                    Effect.logInfo("agent thread stream text delta", {
                      deltaLength: text.length,
                      model: MODEL,
                      threadId,
                    }),
                  ),
                  Stream.ensuring(
                    Effect.logInfo("agent thread stream finalized", {
                      finishReason,
                      model: MODEL,
                      partTypes: Array.from(streamPartTypes),
                      textLength: emittedTextLength,
                      threadId,
                    }),
                  ),
                );
              }),
            ).pipe(
              Stream.catch((error) =>
                Stream.fromEffect(
                  Effect.gen(function* () {
                    if (error instanceof AgentThreadError) {
                      yield* Effect.logError("agent thread stream failed", {
                        threadId,
                        error,
                      });
                      return yield* error;
                    }
                    yield* Effect.logError("agent thread stream failed", {
                      threadId,
                      error,
                    });
                    return yield* new AgentThreadError({
                      operation: "stream",
                      model: MODEL,
                      message: agentThreadErrorMessage(
                        error,
                        "Unable to stream assistant response",
                      ),
                    });
                  }),
                ),
              ),
            ),
        };
      });
    }),
    Effect.provide(Cloudflare.AiGatewayBindingLive),
  ),
) {}

const agentThreadErrorMessage = (error: unknown, fallback: string): string => {
  if (!AiError.isAiError(error)) {
    return fallback;
  }

  if (error.reason._tag === "AuthenticationError" && error.reason.kind === "InvalidKey") {
    return "Cloudflare AI Gateway OpenAI authentication failed. Ensure the gateway has OpenAI access through a provider config or forwarded provider key.";
  }

  return fallback;
};

const partTypes = (parts: ReadonlyArray<Response.AnyPart>): ReadonlyArray<string> =>
  Array.from(new Set(parts.map((part) => part.type)));

const assistantResponseDetail = ({
  finishReason,
  partTypes,
  textLength,
}: {
  readonly finishReason: string;
  readonly partTypes: ReadonlyArray<string>;
  readonly textLength: number;
}): string =>
  `finishReason=${finishReason}; partTypes=${partTypes.join(",") || "none"}; textLength=${textLength}`;

export * as AgentThreadObject from "./ThreadObject.ts";
