import {
  createAssistantMessageEventStream,
  type Api,
  type AssistantMessage,
  type AssistantMessageEventStream,
  type Context as PiContext,
  type Model,
  type SimpleStreamOptions,
  type StopReason,
  type Usage,
} from "@earendil-works/pi-ai";
import * as Layer from "effect/Layer";
import { PiAgentProvider, type ModelOptions } from "../../src/agent-loop/PiAgentProvider.ts";

export interface Call {
  readonly model: string;
  readonly payload: PiContext;
  readonly options: SimpleStreamOptions | undefined;
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
  readonly calls: Call[];
  readonly defaultModel: Model<Api>;
  readonly setScript: (script: Script) => void;
}

export const testModel: Model<Api> = {
  id: "test-model",
  name: "Test Model",
  api: "openai-completions",
  provider: "fake-ai",
  baseUrl: "https://fake-ai.test/v1",
  reasoning: false,
  input: ["text", "image"],
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
  contextWindow: 128_000,
  maxTokens: 4_096,
};

export const make = (
  script: Script = sse(done()),
  options: { readonly defaultModel?: Model<Api> } = {},
): Fake => {
  let currentScript = script;
  return {
    calls: [],
    defaultModel: options.defaultModel ?? testModel,
    setScript: (next) => {
      currentScript = next;
    },
    get script() {
      return currentScript;
    },
  } as Fake & { readonly script: Script };
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

export const layer = (fake: Fake, config?: ModelOptions): Layer.Layer<PiAgentProvider.Service> =>
  PiAgentProvider.layer(config).pipe(
    Layer.provide(
      PiAgentProvider.providerLayer({
        defaultModel: fake.defaultModel,
        stream: (model, context, options) => {
          fake.calls.push({ model: model.id, payload: context, options });
          return streamScript(fake as Fake & { readonly script: Script }, model, options);
        },
      }),
    ),
  );

const streamScript = (
  fake: Fake & { readonly script: Script },
  model: Model<Api>,
  options: SimpleStreamOptions | undefined,
): AssistantMessageEventStream => {
  const stream = createAssistantMessageEventStream();
  void runScript(fake.script, model, stream, options);
  return stream;
};

const runScript = async (
  script: Script,
  model: Model<Api>,
  stream: AssistantMessageEventStream,
  options: SimpleStreamOptions | undefined,
): Promise<void> => {
  const output = assistantMessage(model, []);
  try {
    if (script.type === "throw") throw script.error;
    if (script.type === "response") {
      if (!script.response.ok) {
        throw new Error(
          `Fake provider returned ${script.response.status} ${script.response.statusText}`,
        );
      }
      const text = await script.response.text();
      emitText(stream, output, text);
      finish(stream, output, "stop");
      return;
    }

    stream.push({ type: "start", partial: output });
    let text = "";
    for (const chunk of script.chunks) {
      await delay(chunkDelayMs(chunk), options?.signal);
      if (options?.signal?.aborted) throw abortError();
      const parsed = parseSseChunk(chunkText(chunk));
      const delta = parsed.flatMap(deltaText).join("");
      if (delta.length > 0) {
        if (text.length === 0) {
          output.content.push({ type: "text", text: "" });
          stream.push({ type: "text_start", contentIndex: 0, partial: output });
        }
        text += delta;
        output.content[0] = { type: "text", text };
        stream.push({ type: "text_delta", contentIndex: 0, delta, partial: output });
      }
    }
    if (text.length > 0)
      stream.push({ type: "text_end", contentIndex: 0, content: text, partial: output });
    finish(stream, output, "stop");
  } catch (error) {
    const message = assistantMessage(model, [], "error", errorMessage(error));
    stream.push({ type: "error", reason: "error", error: message });
    stream.end(message);
  }
};

const emitText = (
  stream: AssistantMessageEventStream,
  output: AssistantMessage,
  text: string,
): void => {
  stream.push({ type: "start", partial: output });
  output.content.push({ type: "text", text: "" });
  stream.push({ type: "text_start", contentIndex: 0, partial: output });
  output.content[0] = { type: "text", text };
  stream.push({ type: "text_delta", contentIndex: 0, delta: text, partial: output });
  stream.push({ type: "text_end", contentIndex: 0, content: text, partial: output });
};

const finish = (
  stream: AssistantMessageEventStream,
  output: AssistantMessage,
  reason: Extract<StopReason, "stop" | "length" | "toolUse">,
): void => {
  output.stopReason = reason;
  stream.push({ type: "done", reason, message: output });
  stream.end(output);
};

const assistantMessage = (
  model: Model<Api>,
  content: AssistantMessage["content"],
  stopReason: StopReason = "stop",
  errorMessage?: string,
): AssistantMessage => ({
  role: "assistant",
  content,
  api: model.api,
  provider: model.provider,
  model: model.id,
  usage: emptyUsage(),
  stopReason,
  ...(errorMessage === undefined ? {} : { errorMessage }),
  timestamp: Date.now(),
});

const emptyUsage = (): Usage => ({
  input: 0,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  totalTokens: 0,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
});

const parseSseChunk = (text: string): unknown[] =>
  text
    .split(/\r?\n/)
    .filter((line) => line.startsWith("data:"))
    .map((line) => line.slice(5).trim())
    .filter((data) => data.length > 0 && data !== "[DONE]")
    .flatMap((data) => {
      try {
        return [JSON.parse(data)];
      } catch {
        return [];
      }
    });

const deltaText = (chunk: unknown): string[] => {
  const choices = recordField(chunk, "choices");
  if (!Array.isArray(choices)) return [];
  return choices.flatMap((choice) => {
    const delta = recordField(choice, "delta");
    const content = recordField(delta, "content");
    return typeof content === "string" ? [content] : [];
  });
};

const recordField = (input: unknown, field: string): unknown =>
  typeof input === "object" && input !== null
    ? (input as Record<string, unknown>)[field]
    : undefined;

const chunkDelayMs = (chunk: SseChunk): number =>
  typeof chunk === "string" ? 0 : (chunk.delayMs ?? 0);

const chunkText = (chunk: SseChunk): string => {
  if (typeof chunk === "string") return chunk;
  if ("text" in chunk) return chunk.text;
  return `data: ${JSON.stringify(chunk.data)}\n\n`;
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

const errorMessage = (error: unknown): string =>
  error instanceof Error ? error.message : "Fake provider failed.";

export * as FakeAiProvider from "./FakeAiProvider.ts";
