import { PublicConversationEvent } from "@denora/server/stream-events";
import type { BackoffOptions, LiveMode } from "@durable-streams/client";
import { stream } from "@durable-streams/client";
import * as Schema from "effect/Schema";

import { Auth } from "../lib/Auth.ts";
import { WebConfig } from "../lib/WebConfig.ts";
import type { DenoraConversationEvent } from "./types.ts";

type StreamFetch = (...args: Parameters<typeof globalThis.fetch>) => Promise<Response>;

export interface ConversationStreamOptions {
  readonly conversationId: string;
  readonly offset?: string | undefined;
  readonly tail?: number | undefined;
  readonly live?: LiveMode | undefined;
  readonly signal?: AbortSignal | undefined;
  readonly backoffOptions?: BackoffOptions | undefined;
  readonly baseUrl?: string | undefined;
  readonly fetch?: StreamFetch | undefined;
}

export interface ConversationEventStream extends AsyncIterable<DenoraConversationEvent> {
  cancel(reason?: unknown): void;
  readonly offset: string;
}

export class UnsupportedConversationEventVersionError extends Error {
  readonly received: unknown;
  readonly supported = 3;

  constructor(received: unknown) {
    super(`Denora conversation event version ${String(received)} is unsupported.`);
    this.name = "UnsupportedConversationEventVersionError";
    this.received = received;
  }
}

export function createConversationEventStream(
  options: ConversationStreamOptions,
): ConversationEventStream {
  const decodeEvent = Schema.decodeUnknownSync(PublicConversationEvent);
  const abortController = new AbortController();
  const removeExternalAbortListener = linkAbortSignal(options.signal, abortController);
  const url = conversationEventsUrl(options.conversationId, options.baseUrl);
  if (options.tail !== undefined) url.searchParams.set("tail", String(options.tail));

  let connectOffset = options.offset ?? "-1";
  let currentOffset = connectOffset;
  let responsePromise: Promise<Awaited<ReturnType<typeof stream<unknown>>>> | undefined;
  let started = false;
  let pending:
    | {
        readonly items: readonly unknown[];
        next: number;
        readonly offset: string;
        readonly final: { readonly upToDate: boolean } | undefined;
      }
    | undefined;
  let drained: (() => void) | undefined;
  let notify: (() => void) | undefined;
  let deliveryDone = false;
  let fetchDone = false;
  let finalBatch: { readonly upToDate: boolean; readonly offset: string } | undefined;
  let streamFailure: unknown;

  const wake = () => {
    const resolve = notify;
    notify = undefined;
    resolve?.();
  };
  const releaseBatch = () => {
    const resolve = drained;
    drained = undefined;
    resolve?.();
  };
  const cancel = (reason?: unknown) => {
    abortController.abort(reason);
    removeExternalAbortListener?.();
    releaseBatch();
    wake();
  };
  const connect = () => {
    const fetch = Object.assign(options.fetch ?? fetchWithCredentials, {
      preconnect: globalThis.fetch.preconnect,
    }) satisfies typeof globalThis.fetch;
    const streamOptions = {
      url: url.toString(),
      offset: connectOffset,
      live: options.live ?? "sse",
      json: true,
      signal: abortController.signal,
      fetch,
      warnOnHttp: false,
      ...(options.backoffOptions === undefined ? {} : { backoffOptions: options.backoffOptions }),
    };
    responsePromise ??= stream<unknown>(streamOptions);
    return responsePromise;
  };
  const startConsuming = (res: Awaited<ReturnType<typeof stream<unknown>>>) => {
    res.subscribeJson<unknown>((batch) => {
      if (abortController.signal.aborted) return;
      const final =
        batch.streamClosed || options.live === false ? { upToDate: batch.upToDate } : undefined;
      if (batch.items.length === 0) {
        currentOffset = batch.offset;
        if (final) {
          finalBatch = { ...final, offset: batch.offset };
          deliveryDone = true;
        }
        wake();
        return;
      }
      return new Promise<void>((resolve) => {
        pending = { items: batch.items, next: 0, offset: batch.offset, final };
        drained = resolve;
        wake();
      });
    });
    res.closed.then(
      () => {
        fetchDone = true;
        wake();
      },
      (error: unknown) => {
        streamFailure = error;
        deliveryDone = true;
        wake();
      },
    );
  };
  const nextResult = async (): Promise<IteratorResult<DenoraConversationEvent>> => {
    while (true) {
      if (abortController.signal.aborted) {
        removeExternalAbortListener?.();
        return { value: undefined, done: true };
      }

      if (!started) {
        started = true;
        try {
          startConsuming(await connect());
        } catch (error) {
          started = false;
          removeExternalAbortListener?.();
          if (abortController.signal.aborted || isAbortError(error)) {
            return { value: undefined, done: true };
          }
          throw error;
        }
      }

      if (pending) {
        let value: DenoraConversationEvent;
        try {
          value = decodeConversationEvent(pending.items[pending.next], decodeEvent);
        } catch (error) {
          pending = undefined;
          releaseBatch();
          cancel(error);
          throw error;
        }
        pending.next += 1;
        if (pending.next >= pending.items.length) {
          currentOffset = pending.offset;
          if (pending.final) {
            finalBatch = { ...pending.final, offset: pending.offset };
            deliveryDone = true;
          }
          pending = undefined;
          releaseBatch();
        }
        return { value, done: false };
      }

      if (deliveryDone) {
        if (streamFailure !== undefined) {
          const error = streamFailure;
          streamFailure = undefined;
          removeExternalAbortListener?.();
          if (abortController.signal.aborted || isAbortError(error)) {
            return { value: undefined, done: true };
          }
          throw error;
        }
        if (
          options.live === false &&
          finalBatch &&
          !finalBatch.upToDate &&
          finalBatch.offset !== connectOffset
        ) {
          connectOffset = finalBatch.offset;
          responsePromise = undefined;
          started = false;
          deliveryDone = false;
          fetchDone = false;
          finalBatch = undefined;
          continue;
        }
        removeExternalAbortListener?.();
        return { value: undefined, done: true };
      }

      if (fetchDone && options.live === "sse") {
        await new Promise<void>((resolve) => setTimeout(resolve, 0));
        if (pending || deliveryDone || abortController.signal.aborted) continue;
        removeExternalAbortListener?.();
        return { value: undefined, done: true };
      }

      await new Promise<void>((resolve) => {
        notify = resolve;
      });
    }
  };

  let lastNext: Promise<unknown> | undefined;
  const iterator: AsyncIterator<DenoraConversationEvent> = {
    next() {
      const result = lastNext ? lastNext.then(nextResult, nextResult) : nextResult();
      lastNext = result.catch(() => undefined);
      return result;
    },
    async return() {
      cancel();
      return { value: undefined, done: true };
    },
  };

  return {
    cancel,
    get offset() {
      return currentOffset;
    },
    [Symbol.asyncIterator]() {
      return iterator;
    },
  };
}

const decodeConversationEvent = (
  value: unknown,
  decodeEvent: (value: unknown) => DenoraConversationEvent,
): DenoraConversationEvent => {
  const version =
    value && typeof value === "object" ? (value as { readonly v?: unknown }).v : undefined;
  if (version !== 3) throw new UnsupportedConversationEventVersionError(version);
  return decodeEvent(value);
};

const conversationEventsUrl = (conversationId: string, configuredBaseUrl?: string | undefined) => {
  const baseUrl = configuredBaseUrl ?? WebConfig.requireApiUrl();
  return new URL(`/conversations/${encodeURIComponent(conversationId)}/events`, `${baseUrl}/`);
};

const fetchWithCredentials = Object.assign(
  async (...[input, init]: Parameters<typeof globalThis.fetch>): Promise<Response> => {
    const headers = await Auth.withAuthForwardingHeaders(init?.headers);
    return fetch(input, { ...init, headers, credentials: "include" });
  },
  { preconnect: globalThis.fetch.preconnect },
) satisfies typeof globalThis.fetch;

const linkAbortSignal = (
  signal: AbortSignal | undefined,
  controller: AbortController,
): (() => void) | undefined => {
  if (signal === undefined) return undefined;
  if (signal.aborted) {
    controller.abort(signal.reason);
    return undefined;
  }
  const onAbort = () => controller.abort(signal.reason);
  signal.addEventListener("abort", onAbort, { once: true });
  return () => signal.removeEventListener("abort", onAbort);
};

const isAbortError = (error: unknown): boolean =>
  (error instanceof DOMException && error.name === "AbortError") ||
  (error instanceof Error && error.name === "AbortError");

export * as ChatStream from "./stream.ts";
