import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import { AgentMessageResponse, AgentThreadError } from "../../src/agent/Schema.ts";
import { AgentThreads, type SendInput } from "../../src/agent/Threads.ts";

export const layer = (
  options: {
    readonly send?: ((input: SendInput) => Effect.Effect<AgentMessageResponse>) | undefined;
    readonly stream?: ((input: SendInput) => Stream.Stream<string, AgentThreadError>) | undefined;
  } = {},
) => {
  const send =
    options.send ??
    ((input: SendInput) =>
      Effect.succeed(
        new AgentMessageResponse({
          threadId: input.threadId,
          agentId: input.agentId,
          role: "assistant",
          content: `Mock reply to: ${input.message}`,
        }),
      ));
  const stream =
    options.stream ??
    ((input: SendInput) => Stream.make("Mock ", "stream ", `reply to: ${input.message}`));

  return Layer.succeed(AgentThreads.Service, AgentThreads.Service.of({ send, stream }));
};

export * as AgentThreadsMock from "./AgentThreadsMock.ts";
