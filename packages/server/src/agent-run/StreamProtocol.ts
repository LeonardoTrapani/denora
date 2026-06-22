import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import {
  type EventStreamError,
  type EventStreamReadResult,
  type EventStreamStore,
  formatOffset,
  parseOffset,
} from "./EventStreamStore.ts";

const DEFAULT_LONG_POLL_TIMEOUT_MS = 30_000;
const DEFAULT_SSE_HEARTBEAT_MS = 15_000;
const CURSOR_EPOCH_MS = 1728432000000;
const CURSOR_INTERVAL_MS = 20_000;

export const STREAM_NEXT_OFFSET = "Stream-Next-Offset";
export const STREAM_UP_TO_DATE = "Stream-Up-To-Date";
export const STREAM_CLOSED = "Stream-Closed";
export const STREAM_CURSOR = "Stream-Cursor";

const SECURITY_HEADERS = {
  "X-Content-Type-Options": "nosniff",
  "Cross-Origin-Resource-Policy": "cross-origin",
};

const SSE_OFFSET_FIELD = "streamNextOffset";
const SSE_CURSOR_FIELD = "streamCursor";
const SSE_CLOSED_FIELD = "streamClosed";
const SSE_UP_TO_DATE_FIELD = "upToDate";

export class InvalidStreamRequest extends Schema.TaggedErrorClass<InvalidStreamRequest>()(
  "InvalidStreamRequest",
  { reason: Schema.String },
) {}

export interface HandleStreamReadOptions {
  readonly store: EventStreamStore;
  readonly path: string;
  readonly request: Request;
  readonly longPollTimeoutMs?: number | undefined;
  readonly sseHeartbeatMs?: number | undefined;
}

export const handleRunObjectRequest = Effect.fn("StreamProtocol.handleRunObjectRequest")(function* (
  store: EventStreamStore,
  request: Request,
): Effect.fn.Return<Response, EventStreamError> {
  const url = new URL(request.url);
  const runId = runIdFromPath(url.pathname);
  const path = `runs/${runId}`;

  if (request.method === "HEAD") return yield* handleStreamHead(store, path);
  if (request.method === "GET") return yield* handleStreamRead({ store, path, request });

  return new Response(JSON.stringify(errorBody("method_not_allowed", "Method not allowed.")), {
    status: 405,
    headers: { "content-type": "application/json", Allow: "GET, HEAD", ...SECURITY_HEADERS },
  });
});

export const handleStreamHead = Effect.fn("StreamProtocol.handleStreamHead")(function* (
  store: EventStreamStore,
  path: string,
): Effect.fn.Return<Response, EventStreamError> {
  const meta = yield* store.getStreamMeta(path);
  if (meta === null) return notFoundResponse(path, true);

  const headers: Record<string, string> = {
    "content-type": "application/json",
    ...SECURITY_HEADERS,
    [STREAM_NEXT_OFFSET]: meta.nextOffset,
    [STREAM_UP_TO_DATE]: "true",
    "cache-control": "no-store",
    etag: generateETag(path, "-1", meta.nextOffset, meta.closed),
  };
  if (meta.closed) headers[STREAM_CLOSED] = "true";
  return new Response(null, { status: 200, headers });
});

export const handleStreamRead = Effect.fn("StreamProtocol.handleStreamRead")(function* (
  options: HandleStreamReadOptions,
): Effect.fn.Return<Response, EventStreamError> {
  const { store, path, request } = options;
  const url = new URL(request.url);
  const offsetValues = url.searchParams.getAll("offset");
  const offsetParam = offsetValues[0] ?? "-1";
  const tailValues = url.searchParams.getAll("tail");
  const tailParam = tailValues[0];
  const liveRaw = url.searchParams.get("live");
  const cursor = url.searchParams.get("cursor") ?? undefined;

  const invalid = validateReadParams(offsetValues, offsetParam, tailValues, liveRaw);
  if (invalid !== undefined) return invalidRequestResponse(invalid.reason);

  const meta = yield* store.getStreamMeta(path);
  if (meta === null) return notFoundResponse(path, false);

  const readOffset = yield* resolveReadOffset(offsetParam, liveRaw, tailParam, meta.nextOffset);

  if (liveRaw === "sse") {
    return yield* sseResponse(store, path, readOffset, request.signal, options.sseHeartbeatMs);
  }

  const result = yield* store.readEvents(path, { offset: readOffset });
  if (liveRaw === "long-poll") {
    return yield* handleLongPollMode(
      store,
      path,
      readOffset,
      readOffset,
      cursor,
      result,
      request.signal,
      options.longPollTimeoutMs,
    );
  }

  return catchUpResponse(request, path, readOffset, result);
});

const resolveReadOffset = Effect.fn("StreamProtocol.resolveReadOffset")(function* (
  offsetParam: string,
  live: string | null,
  tailParam: string | undefined,
  metaNextOffset: string,
): Effect.fn.Return<string, EventStreamError> {
  if (offsetParam === "now" && live !== null) return metaNextOffset;
  if (offsetParam === "-1" && tailParam !== undefined) {
    const tail = BigInt(tailParam);
    const last = BigInt(yield* parseOffset(metaNextOffset));
    const start = last - tail > -1n ? last - tail : -1n;
    return formatOffset(Number(start));
  }
  return offsetParam;
});

const handleLongPollMode = Effect.fn("StreamProtocol.handleLongPollMode")(function* (
  store: EventStreamStore,
  path: string,
  readOffset: string,
  requestOffset: string,
  clientCursor: string | undefined,
  result: EventStreamReadResult,
  signal: AbortSignal,
  timeoutMs = DEFAULT_LONG_POLL_TIMEOUT_MS,
): Effect.fn.Return<Response, EventStreamError> {
  if (result.events.length > 0)
    return longPollDataResponse(result, path, requestOffset, clientCursor);
  if (result.closed && result.upToDate) {
    return longPollEmptyResponse(result.nextOffset, clientCursor, true);
  }

  const waitResult = yield* waitForStreamData(store, path, signal, timeoutMs, () =>
    store
      .readEvents(path, { offset: readOffset })
      .pipe(Effect.map((reread) => reread.events.length > 0 || (reread.closed && reread.upToDate))),
  );

  if (waitResult === "aborted")
    return new Response(null, { status: 499, headers: SECURITY_HEADERS });
  if (waitResult === "timeout") {
    const closed = ((yield* store.getStreamMeta(path))?.closed ?? false) === true;
    return longPollEmptyResponse(result.nextOffset, clientCursor, closed);
  }

  const freshResult = yield* store.readEvents(path, { offset: readOffset });
  if (freshResult.events.length > 0) {
    return longPollDataResponse(freshResult, path, requestOffset, clientCursor);
  }
  const closed = ((yield* store.getStreamMeta(path))?.closed ?? false) === true;
  return longPollEmptyResponse(result.nextOffset, clientCursor, closed);
});

const sseResponse = Effect.fn("StreamProtocol.sseResponse")(function* (
  store: EventStreamStore,
  path: string,
  offset: string,
  signal: AbortSignal,
  heartbeatMs = DEFAULT_SSE_HEARTBEAT_MS,
): Effect.fn.Return<Response, never> {
  const body = yield* Stream.toReadableStreamEffect(
    Stream.encodeText(sseFrames(store, path, offset, signal, heartbeatMs)),
  );
  return new Response(body, {
    status: 200,
    headers: {
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
      ...SECURITY_HEADERS,
    },
  });
});

const sseFrames = (
  store: EventStreamStore,
  path: string,
  offset: string,
  signal: AbortSignal,
  heartbeatMs: number,
): Stream.Stream<string> =>
  Stream.callback<string>(
    Effect.fn("StreamProtocol.sseFrames")(function* (queue) {
      const wakeups = yield* Queue.sliding<void>(1);
      let currentOffset = offset;
      let connected = !signal.aborted;

      const push = (frame: string) => {
        if (connected) Queue.offerUnsafe(queue, frame);
      };
      const wake = () => {
        if (connected) Queue.offerUnsafe(wakeups, undefined);
      };
      const stop = () => {
        if (!connected) return;
        connected = false;
        Queue.endUnsafe(queue);
      };

      const readAvailable = Effect.fn("StreamProtocol.sseReadAvailable")(function* () {
        while (connected) {
          const keepReading = yield* store.readEvents(path, { offset: currentOffset }).pipe(
            Effect.matchEffect({
              onFailure: (error) =>
                Effect.sync(() => {
                  push(
                    `event: error\n${encodeSseData(JSON.stringify({ message: String(error) }))}`,
                  );
                  stop();
                  return false;
                }),
              onSuccess: (result) =>
                Effect.sync(() => {
                  if (result.events.length > 0) {
                    push(
                      `event: data\n${encodeSseData(JSON.stringify(publicEventData(path, result)))}`,
                    );
                  }

                  const streamClosed = result.closed && result.upToDate;
                  const controlData: Record<string, string | boolean> = {
                    [SSE_OFFSET_FIELD]: result.nextOffset,
                  };
                  if (streamClosed) {
                    controlData[SSE_CLOSED_FIELD] = true;
                  } else {
                    controlData[SSE_CURSOR_FIELD] = generateCursor();
                    if (result.upToDate) controlData[SSE_UP_TO_DATE_FIELD] = true;
                  }
                  push(`event: control\n${encodeSseData(JSON.stringify(controlData))}`);
                  currentOffset = result.nextOffset;
                  if (streamClosed) stop();
                  return connected && !result.upToDate;
                }),
            }),
          );
          if (!keepReading) return;
        }
      });

      const unsubscribe = yield* store.subscribe(path, wake);
      const onAbort = () => stop();
      signal.addEventListener("abort", onAbort, { once: true });
      const heartbeat = setInterval(() => {
        if (!connected) return;
        push(": heartbeat\n\n");
        wake();
      }, heartbeatMs);

      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          connected = false;
          clearInterval(heartbeat);
          signal.removeEventListener("abort", onAbort);
          unsubscribe();
        }),
      );

      yield* Effect.forkScoped(
        Effect.gen(function* () {
          while (connected) {
            yield* Queue.take(wakeups);
            yield* readAvailable();
          }
        }),
      );

      if (signal.aborted) stop();
      else wake();
    }),
  );

const waitForStreamData = (
  store: EventStreamStore,
  path: string,
  signal: AbortSignal,
  timeoutMs: number,
  recheck: () => Effect.Effect<boolean, EventStreamError>,
): Effect.Effect<"data" | "timeout" | "aborted", EventStreamError> =>
  Effect.gen(function* () {
    if (signal.aborted) return "aborted";

    const wakeups = yield* Queue.sliding<"data" | "timeout" | "aborted">(1);
    const offer = (result: "data" | "timeout" | "aborted") => {
      Queue.offerUnsafe(wakeups, result);
    };

    return yield* Effect.acquireUseRelease(
      Effect.gen(function* () {
        const unsubscribe = yield* store.subscribe(path, () => offer("data"));
        const timer = setTimeout(() => offer("timeout"), timeoutMs);
        const onAbort = () => offer("aborted");
        signal.addEventListener("abort", onAbort, { once: true });

        return () => {
          unsubscribe();
          clearTimeout(timer);
          signal.removeEventListener("abort", onAbort);
        };
      }),
      () =>
        Effect.gen(function* () {
          const hasData = yield* recheck();
          if (hasData) return "data";
          return yield* Queue.take(wakeups);
        }),
      (cleanup) => Effect.sync(cleanup),
    );
  });

const validateReadParams = (
  offsetValues: ReadonlyArray<string>,
  offsetParam: string,
  tailValues: ReadonlyArray<string>,
  liveRaw: string | null,
): InvalidStreamRequest | undefined => {
  if (offsetValues.length > 1) {
    return new InvalidStreamRequest({ reason: "Duplicate offset parameters are not allowed." });
  }
  if (tailValues.length > 1) {
    return new InvalidStreamRequest({ reason: "Duplicate tail parameters are not allowed." });
  }
  const tailParam = tailValues[0];
  if (tailParam !== undefined && !/^[1-9]\d*$/.test(tailParam)) {
    return new InvalidStreamRequest({
      reason: "Tail must be an integer greater than or equal to 1.",
    });
  }
  if (liveRaw !== null && offsetValues.length === 0) {
    return new InvalidStreamRequest({ reason: "Offset is required for live mode." });
  }
  if (liveRaw !== null && liveRaw !== "long-poll" && liveRaw !== "sse") {
    return new InvalidStreamRequest({ reason: 'Invalid live mode. Use "long-poll" or "sse".' });
  }
  if (offsetParam !== "-1" && offsetParam !== "now" && !/^\d+_\d+$/.test(offsetParam)) {
    return new InvalidStreamRequest({ reason: "Invalid offset format." });
  }
  return undefined;
};

const catchUpResponse = (
  request: Request,
  path: string,
  offsetParam: string,
  result: EventStreamReadResult,
): Response => {
  const isClosed = result.closed && result.upToDate;
  const etag =
    offsetParam === "now"
      ? undefined
      : generateETag(path, offsetParam, result.nextOffset, isClosed);
  if (etag !== undefined && request.headers.get("if-none-match") === etag) {
    return new Response(null, { status: 304, headers: { etag, ...SECURITY_HEADERS } });
  }
  const headers: Record<string, string> = {
    "content-type": "application/json",
    [STREAM_NEXT_OFFSET]: result.nextOffset,
    "cache-control": "no-store",
    ...SECURITY_HEADERS,
  };
  if (etag !== undefined) headers.etag = etag;
  if (result.upToDate) headers[STREAM_UP_TO_DATE] = "true";
  if (isClosed) headers[STREAM_CLOSED] = "true";
  return new Response(JSON.stringify(publicEventData(path, result)), { status: 200, headers });
};

const longPollDataResponse = (
  result: EventStreamReadResult,
  path: string,
  offsetParam: string,
  clientCursor: string | undefined,
): Response => {
  const isClosed = result.closed && result.upToDate;
  const headers: Record<string, string> = {
    "content-type": "application/json",
    "cache-control": "no-store",
    ...SECURITY_HEADERS,
    [STREAM_NEXT_OFFSET]: result.nextOffset,
    [STREAM_CURSOR]: generateCursor(clientCursor),
  };
  if (result.upToDate) headers[STREAM_UP_TO_DATE] = "true";
  if (isClosed) headers[STREAM_CLOSED] = "true";
  if (offsetParam !== "now")
    headers.etag = generateETag(path, offsetParam, result.nextOffset, isClosed);
  return new Response(JSON.stringify(publicEventData(path, result)), { status: 200, headers });
};

const longPollEmptyResponse = (
  nextOffset: string,
  clientCursor: string | undefined,
  closed: boolean,
): Response => {
  const headers: Record<string, string> = {
    ...SECURITY_HEADERS,
    [STREAM_NEXT_OFFSET]: nextOffset,
    [STREAM_UP_TO_DATE]: "true",
    [STREAM_CURSOR]: generateCursor(clientCursor),
  };
  if (closed) headers[STREAM_CLOSED] = "true";
  return new Response(null, { status: 204, headers });
};

const publicEventData = (path: string, result: EventStreamReadResult): ReadonlyArray<unknown> =>
  result.events.map((event) =>
    path.startsWith("runs/") ? normalizeRunStreamEvent(event.data) : event.data,
  );

const normalizeRunStreamEvent = (value: unknown): unknown => {
  if (value === null || typeof value !== "object") return value;
  const event = value as Record<string, unknown>;
  if (event.type !== "run_start" || "input" in event || !("payload" in event)) return value;
  const { payload, ...rest } = event;
  return { ...rest, input: payload };
};

const invalidRequestResponse = (reason: string): Response =>
  new Response(JSON.stringify(errorBody("invalid_request", reason)), {
    status: 400,
    headers: { "content-type": "application/json", ...SECURITY_HEADERS },
  });

const notFoundResponse = (path: string, head: boolean): Response => {
  const body = errorBody(
    "run_not_found",
    `Agent Run "${path.slice("runs/".length)}" was not found.`,
  );
  return new Response(head ? null : JSON.stringify(body), {
    status: 404,
    headers: { "content-type": "application/json", ...SECURITY_HEADERS },
  });
};

const errorBody = (type: string, message: string) => ({ error: { type, message } });

const runIdFromPath = (pathname: string): string => {
  const match = /^\/runs\/([^/]+)$/.exec(pathname);
  return decodeURIComponent(match?.[1] ?? "");
};

const encodeSseData = (payload: string): string =>
  `${payload
    .split(/\r\n|\r|\n/)
    .map((line) => `data:${line}`)
    .join("\n")}\n\n`;

const generateCursor = (clientCursor?: string): string => {
  const currentInterval = Math.floor((Date.now() - CURSOR_EPOCH_MS) / CURSOR_INTERVAL_MS);
  if (clientCursor === undefined) return String(currentInterval);
  const clientInterval = Number.parseInt(clientCursor, 10);
  if (!Number.isFinite(clientInterval) || clientInterval < currentInterval)
    return String(currentInterval);
  return String(clientInterval + Math.floor(Math.random() * 180) + 1);
};

const generateETag = (
  path: string,
  startOffset: string,
  endOffset: string,
  closed: boolean,
): string => {
  const pathEncoded = btoa(String.fromCharCode(...new TextEncoder().encode(path)));
  return `"${pathEncoded}:${startOffset}:${endOffset}${closed ? ":c" : ""}"`;
};

export * as StreamProtocol from "./StreamProtocol.ts";
