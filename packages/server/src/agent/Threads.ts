import { RuntimeContext, type BaseRuntimeContext } from "alchemy";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import { AgentMessageResponse, AgentThreadError } from "./Schema.ts";
import { AgentThreadObject } from "./ThreadObject.ts";

export interface SendInput {
  readonly agentId: string;
  readonly threadId: string;
  readonly message: string;
}

export interface Interface {
  readonly send: (input: SendInput) => Effect.Effect<AgentMessageResponse, AgentThreadError>;
  readonly stream: (input: SendInput) => Stream.Stream<string, AgentThreadError>;
}

export class Service extends Context.Service<Service, Interface>()("@denora/server/AgentThreads") {}

export type Namespace = Effect.Success<typeof AgentThreadObject.ThreadObject>;

export const layer = (
  namespace: Namespace,
  runtimeContext: BaseRuntimeContext,
): Layer.Layer<Service> =>
  Layer.succeed(
    Service,
    Service.of({
      send: (input) =>
        namespace
          .getByName(objectName(input))
          .send(input.threadId, input.message)
          .pipe(
            Effect.provideService(RuntimeContext, runtimeContext),
            Effect.map(
              (reply) =>
                new AgentMessageResponse({
                  threadId: input.threadId,
                  agentId: input.agentId,
                  role: "assistant",
                  content: reply.content,
                }),
            ),
          ),
      stream: (input) =>
        namespace
          .getByName(objectName(input))
          .stream(input.threadId, input.message)
          .pipe(Stream.provideService(RuntimeContext, runtimeContext)),
    }),
  );

const objectName = (input: SendInput): string => `${input.agentId}:${input.threadId}`;

export * as AgentThreads from "./Threads.ts";
