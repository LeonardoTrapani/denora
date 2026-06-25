import type { BackoffOptions, LiveMode } from "@durable-streams/client";
import { stream } from "@durable-streams/client";

import { WebConfig } from "../lib/WebConfig.ts";
import { withAuthForwardingHeaders } from "../lib/request-auth-headers";
import type { DenoraConversationEvent } from "./types.ts";

export interface ConversationStreamOptions {
  readonly conversationId: string;
  readonly offset?: string | undefined;
  readonly tail?: number | undefined;
  readonly live?: LiveMode | undefined;
  readonly signal?: AbortSignal | undefined;
  readonly backoffOptions?: BackoffOptions | undefined;
}

export interface ConversationEventStream<T = DenoraConversationEvent> extends AsyncIterable<T> {
  cancel(reason?: unknown): void;
  readonly offset: string;
}

export function createConversationEventStream<T = DenoraConversationEvent>(
  options: ConversationStreamOptions,
): ConversationEventStream<T> {
  const abortController = new AbortController();
  const removeExternalAbortListener = linkAbortSignal(options.signal, abortController);
  const url = conversationEventsUrl(options.conversationId);
  if (options.tail !== undefined) url.searchParams.set("tail", String(options.tail));

  let connectOffset = options.offset ?? "-1";
  let currentOffset = connectOffset;
  let responsePromise: Promise<Awaited<ReturnType<typeof stream<T>>>> | undefined;
  let started = false;
  let pending:
    | {
        readonly items: readonly T[];
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
    const streamOptions = {
      url: url.toString(),
      offset: connectOffset,
      live: options.live ?? true,
      json: true,
      signal: abortController.signal,
      fetch: fetchWithCredentials,
      warnOnHttp: false,
      ...(options.backoffOptions === undefined ? {} : { backoffOptions: options.backoffOptions }),
    };
    responsePromise ??= stream<T>(streamOptions);
    return responsePromise;
  };
  const startConsuming = (res: Awaited<ReturnType<typeof stream<T>>>) => {
    res.subscribeJson<T>((batch) => {
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
  const nextResult = async (): Promise<IteratorResult<T>> => {
    while (true) {
      if (abortController.signal.aborted) {
        removeExternalAbortListener?.();
        return { value: undefined as T, done: true };
      }

      if (!started) {
        started = true;
        try {
          startConsuming(await connect());
        } catch (error) {
          started = false;
          removeExternalAbortListener?.();
          if (abortController.signal.aborted || isAbortError(error)) {
            return { value: undefined as T, done: true };
          }
          throw error;
        }
      }

      if (pending) {
        const value = pending.items[pending.next] as T;
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
            return { value: undefined as T, done: true };
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
        return { value: undefined as T, done: true };
      }

      if (fetchDone && options.live === "sse") {
        await new Promise<void>((resolve) => setTimeout(resolve, 0));
        if (pending || deliveryDone || abortController.signal.aborted) continue;
        removeExternalAbortListener?.();
        return { value: undefined as T, done: true };
      }

      await new Promise<void>((resolve) => {
        notify = resolve;
      });
    }
  };

  let lastNext: Promise<unknown> | undefined;
  const iterator: AsyncIterator<T> = {
    next() {
      const result = lastNext ? lastNext.then(nextResult, nextResult) : nextResult();
      lastNext = result.catch(() => undefined);
      return result;
    },
    async return() {
      cancel();
      return { value: undefined as T, done: true };
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

const conversationEventsUrl = (conversationId: string) => {
  const baseUrl = WebConfig.requireApiUrl();
  return new URL(`/conversations/${encodeURIComponent(conversationId)}/events`, `${baseUrl}/`);
};

const fetchWithCredentials: typeof globalThis.fetch = async (input, init) => {
  const headers = await withAuthForwardingHeaders(init?.headers);
  return fetch(input, { ...init, headers, credentials: "include" });
};

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
