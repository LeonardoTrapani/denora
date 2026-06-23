import type * as Cloudflare from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { PiAgentModel, type AiGatewayModelOptions } from "../../src/agent-loop/PiAgentModel.ts";

export interface Call {
  readonly model: string;
  readonly payload: Record<string, unknown>;
  readonly options: Record<string, unknown> | undefined;
}

export type SseChunk =
  | string
  | {
      readonly text: string;
      readonly delayMs?: number | undefined;
    }
  | {
      readonly data: unknown;
      readonly delayMs?: number | undefined;
    };

export type Script =
  | {
      readonly type: "sse";
      readonly chunks: ReadonlyArray<SseChunk>;
      readonly init?: ResponseInit | undefined;
    }
  | {
      readonly type: "response";
      readonly response: Response;
    }
  | {
      readonly type: "throw";
      readonly error: unknown;
    };

export interface Fake {
  readonly ai: Effect.Success<Cloudflare.AiGatewayClient["raw"]>;
  readonly client: Cloudflare.AiGatewayClient;
  readonly calls: Call[];
  readonly gatewayId: string;
  readonly setScript: (script: Script) => void;
}

export const make = (
  script: Script = sse(done()),
  options: { readonly id?: string } = {},
): Fake => {
  let currentScript = script;
  const calls: Call[] = [];
  const gatewayId = options.id ?? "test-gateway";
  const raw = {
    run: async (
      model: string,
      payload: Record<string, unknown>,
      runOptions?: Record<string, unknown>,
    ) => {
      calls.push({ model, payload, options: runOptions });
      if (currentScript.type === "throw") throw currentScript.error;
      if (currentScript.type === "response") return currentScript.response;
      return createSseResponse(currentScript.chunks, currentScript.init, signalFrom(runOptions));
    },
  };

  const client = {
    raw: Effect.succeed(raw),
    gateway: Effect.succeed({}),
    id: Effect.succeed(gatewayId),
    patchLog: () => Effect.void,
    getLog: () => Effect.die(new Error("FakeAiGateway.getLog is not implemented.")),
    getUrl: () => Effect.succeed(`https://gateway.test/${gatewayId}`),
    run: () => Effect.die(new Error("FakeAiGateway.run is not implemented.")),
    model: () => Effect.die(new Error("FakeAiGateway.model is not implemented.")),
  } as unknown as Cloudflare.AiGatewayClient;

  return {
    ai: raw as Effect.Success<Cloudflare.AiGatewayClient["raw"]>,
    client,
    calls,
    gatewayId,
    setScript: (next) => {
      currentScript = next;
    },
  };
};

export const sse = (...chunks: ReadonlyArray<SseChunk>): Script => ({
  type: "sse",
  chunks,
  init: { headers: { "content-type": "text/event-stream" } },
});

export const json = (data: unknown, delayMs?: number): SseChunk => ({ data, delayMs });

export const raw = (text: string, delayMs?: number): SseChunk => ({ text, delayMs });

export const done = (delayMs?: number): SseChunk => raw("data: [DONE]\n\n", delayMs);

export const response = (value: Response): Script => ({ type: "response", response: value });

export const nonOk = (body: string | null, init: ResponseInit): Script =>
  response(new Response(body, init));

export const emptyBody = (): Script => response(new Response(null));

export const throws = (error: unknown): Script => ({ type: "throw", error });

export const layer = (
  fake: Fake,
  config?: AiGatewayModelOptions,
): Layer.Layer<PiAgentModel.Service> =>
  PiAgentModel.layer(config).pipe(
    Layer.provide(
      Layer.succeed(
        PiAgentModel.AiGateway,
        PiAgentModel.AiGateway.of({
          ai: fake.ai,
          id: PiAgentModel.AiGatewayId.make(fake.gatewayId),
        }),
      ),
    ),
  );

const createSseResponse = (
  chunks: ReadonlyArray<SseChunk>,
  init: ResponseInit | undefined,
  signal: AbortSignal | undefined,
): Response => {
  const encoder = new TextEncoder();
  return new Response(
    new ReadableStream<Uint8Array>({
      async start(controller) {
        try {
          for (const chunk of chunks) {
            await delay(chunkDelayMs(chunk), signal);
            if (signal?.aborted) throw abortError();
            controller.enqueue(encoder.encode(chunkText(chunk)));
          }
          controller.close();
        } catch (error) {
          controller.error(error);
        }
      },
    }),
    init,
  );
};

const chunkDelayMs = (chunk: SseChunk): number =>
  typeof chunk === "string" ? 0 : (chunk.delayMs ?? 0);

const chunkText = (chunk: SseChunk): string => {
  if (typeof chunk === "string") return chunk;
  if ("text" in chunk) return chunk.text;
  return `data: ${JSON.stringify(chunk.data)}\n\n`;
};

const signalFrom = (options: Record<string, unknown> | undefined): AbortSignal | undefined => {
  const signal = options?.signal;
  return signal instanceof AbortSignal ? signal : undefined;
};

const delay = (ms: number, signal: AbortSignal | undefined): Promise<void> =>
  new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(abortError());
      return;
    }
    if (ms <= 0) {
      resolve();
      return;
    }

    const timeout = setTimeout(() => {
      cleanup();
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(timeout);
      cleanup();
      reject(abortError());
    };
    const cleanup = () => signal?.removeEventListener("abort", onAbort);
    signal?.addEventListener("abort", onAbort, { once: true });
  });

const abortError = (): DOMException => new DOMException("The operation was aborted.", "AbortError");

export * as FakeAiGateway from "./FakeAiGateway.ts";
