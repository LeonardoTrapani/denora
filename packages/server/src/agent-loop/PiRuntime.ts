import type { AgentTool, StreamFn } from "@earendil-works/pi-agent-core";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ManagedRuntime from "effect/ManagedRuntime";
import { PiAgentProvider } from "./PiAgentProvider.ts";

export interface Interface {
  readonly streamFn: StreamFn;
  readonly tools?: ReadonlyArray<AgentTool<any>> | undefined;
}

export class Service extends Context.Service<Service, Interface>()("@denora/server/PiRuntime") {}

export const layer: Layer.Layer<Service, never, PiAgentProvider.Service> = Layer.effect(
  Service,
  Effect.gen(function* () {
    const modelService = yield* PiAgentProvider.Service;
    const runtime = ManagedRuntime.make(Layer.succeed(PiAgentProvider.Service, modelService));

    const streamFn: StreamFn = (model, context, options) =>
      runtime.runPromise(
        Effect.gen(function* () {
          const service = yield* PiAgentProvider.Service;
          return yield* service.stream({ model, context, options });
        }),
      );

    return Service.of({ streamFn });
  }),
);

export * as PiRuntime from "./PiRuntime.ts";
