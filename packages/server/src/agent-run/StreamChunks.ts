import type * as Cloudflare from "alchemy/Cloudflare";
import type { AssistantMessage, AssistantMessageEvent } from "@earendil-works/pi-ai";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { SqlStorage } from "./SqlStorage.ts";

export const STREAM_FLUSH_INTERVAL = "3 seconds";
export const MAX_STREAM_CHUNK_SEGMENT_BYTES = 1_900_000;

const CREATE_STREAM_CHUNK_SEGMENTS_TABLE = `
CREATE TABLE IF NOT EXISTS denora_stream_chunk_segments (
  stream_key     TEXT NOT NULL,
  segment_index  INTEGER NOT NULL,
  body           TEXT NOT NULL,
  created_at     TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (stream_key, segment_index)
)`;

const textEncoder = new TextEncoder();

export class StreamChunkSegmentTooLarge extends Schema.TaggedErrorClass<StreamChunkSegmentTooLarge>()(
  "StreamChunkSegmentTooLarge",
  {
    serializedBytes: Schema.Number,
    maximumBytes: Schema.Number,
  },
) {}

export class StreamChunkStorageFailed extends Schema.TaggedErrorClass<StreamChunkStorageFailed>()(
  "StreamChunkStorageFailed",
  {
    operation: Schema.String,
    cause: Schema.Defect(),
  },
) {}

export type StreamChunkError = StreamChunkSegmentTooLarge | StreamChunkStorageFailed;

export interface StreamChunkSegment {
  readonly segmentIndex: number;
  readonly body: string;
}

export interface SignalMessage {
  readonly role: "signal";
  readonly type: string;
  readonly content: string;
  readonly attributes?: Readonly<Record<string, unknown>> | undefined;
  readonly timestamp: number;
}

export interface ReconstructedInterruptedStream {
  readonly partial: AssistantMessage;
  readonly interrupted: SignalMessage;
  readonly continued: SignalMessage;
}

export interface StreamChunkStore {
  readonly appendStreamChunkSegment: (
    streamKey: string,
    segmentIndex: number,
    body: string,
  ) => Effect.Effect<boolean, StreamChunkError>;
  readonly readStreamChunkSegments: (
    streamKey: string,
  ) => Effect.Effect<ReadonlyArray<StreamChunkSegment>, StreamChunkStorageFailed>;
  readonly deleteStreamChunkSegments: (
    streamKey: string,
  ) => Effect.Effect<void, StreamChunkStorageFailed>;
}

export class Service extends Context.Service<Service, StreamChunkStore>()(
  "@denora/server/StreamChunks",
) {}

export const sqliteLayer: Layer.Layer<Service, StreamChunkStorageFailed, SqlStorage.Service> =
  Layer.effect(
    Service,
    Effect.gen(function* () {
      const sql = yield* SqlStorage.Service;
      const store = yield* makeSqliteStreamChunkStore(sql);
      return Service.of(store);
    }),
  );

interface InsertedRow extends Record<string, Cloudflare.SqlStorageValue> {
  readonly segment_index: number;
}

interface SegmentRow extends Record<string, Cloudflare.SqlStorageValue> {
  readonly segment_index: number;
  readonly body: string;
}

const storageFailure =
  (operation: string) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, StreamChunkStorageFailed, R> =>
    effect.pipe(Effect.mapError((cause) => new StreamChunkStorageFailed({ operation, cause })));

export const makeSqliteStreamChunkStore = Effect.fn("StreamChunks.makeSqliteStreamChunkStore")(
  function* (
    sql: Cloudflare.SqlStorage,
  ): Effect.fn.Return<StreamChunkStore, StreamChunkStorageFailed> {
    yield* ensureTables(sql);

    const appendStreamChunkSegment = Effect.fn("StreamChunks.appendSqliteStreamChunkSegment")(
      function* (
        streamKey: string,
        segmentIndex: number,
        body: string,
      ): Effect.fn.Return<boolean, StreamChunkError> {
        const serializedBytes = textEncoder.encode(body).byteLength;
        if (serializedBytes > MAX_STREAM_CHUNK_SEGMENT_BYTES) {
          return yield* new StreamChunkSegmentTooLarge({
            serializedBytes,
            maximumBytes: MAX_STREAM_CHUNK_SEGMENT_BYTES,
          });
        }

        const cursor = yield* sql
          .exec<InsertedRow>(
            `INSERT OR IGNORE INTO denora_stream_chunk_segments
             (stream_key, segment_index, body)
             VALUES (?, ?, ?)
             RETURNING segment_index`,
            streamKey,
            segmentIndex,
            body,
          )
          .pipe(storageFailure("append stream chunk segment"));
        const rows = yield* cursor
          .toArray()
          .pipe(storageFailure("collect appended stream chunk segment"));
        return rows[0] !== undefined;
      },
    );

    const readStreamChunkSegments = Effect.fn("StreamChunks.readSqliteStreamChunkSegments")(
      function* (
        streamKey: string,
      ): Effect.fn.Return<ReadonlyArray<StreamChunkSegment>, StreamChunkStorageFailed> {
        const cursor = yield* sql
          .exec<SegmentRow>(
            `SELECT segment_index, body
             FROM denora_stream_chunk_segments
             WHERE stream_key = ?
             ORDER BY segment_index ASC`,
            streamKey,
          )
          .pipe(storageFailure("read stream chunk segments"));
        const rows = yield* cursor.toArray().pipe(storageFailure("collect stream chunk segments"));
        return rows.map((row) => ({ segmentIndex: row.segment_index, body: row.body }));
      },
    );

    const deleteStreamChunkSegments = (
      streamKey: string,
    ): Effect.Effect<void, StreamChunkStorageFailed> =>
      sql
        .exec(`DELETE FROM denora_stream_chunk_segments WHERE stream_key = ?`, streamKey)
        .pipe(storageFailure("delete stream chunk segments"), Effect.asVoid);

    return {
      appendStreamChunkSegment,
      readStreamChunkSegments,
      deleteStreamChunkSegments,
    } satisfies StreamChunkStore;
  },
);

export const ensureTables = (
  sql: Cloudflare.SqlStorage,
): Effect.Effect<void, StreamChunkStorageFailed> =>
  sql
    .exec(CREATE_STREAM_CHUNK_SEGMENTS_TABLE)
    .pipe(storageFailure("create stream chunk segments table"), Effect.asVoid);

export type AssistantStreamChunkEvent = AssistantMessageEvent;

export interface StreamChunkWriter {
  readonly streamKey: string;
  readonly write: (event: AssistantStreamChunkEvent) => Effect.Effect<void, StreamChunkError>;
  readonly flush: () => Effect.Effect<void, StreamChunkError>;
  readonly close: () => Effect.Effect<void, StreamChunkError>;
  readonly cancel: () => Effect.Effect<void>;
  readonly isFailed: () => boolean;
}

export const makeStreamChunkWriter = (
  store: Pick<StreamChunkStore, "appendStreamChunkSegment">,
  streamKey: string,
): StreamChunkWriter => {
  let pending: CompactStreamEvent[] = [];
  let pendingPartial: AssistantMessage | undefined;
  let segmentIndex = 0;
  let failed = false;
  let active = true;
  let scheduledFlush: Fiber.Fiber<void, never> | undefined;
  let activeFlush: Fiber.Fiber<void, StreamChunkError> | undefined;

  const cancelScheduledFlush = Effect.fn("StreamChunks.StreamChunkWriter.cancelScheduledFlush")(
    function* (): Effect.fn.Return<void> {
      const fiber = scheduledFlush;
      scheduledFlush = undefined;
      if (fiber !== undefined) yield* Fiber.interrupt(fiber);
    },
  );

  const scheduleFlush = Effect.fn("StreamChunks.StreamChunkWriter.scheduleFlush")(
    function* (): Effect.fn.Return<void> {
      if (!active || failed || pending.length === 0 || scheduledFlush !== undefined) return;
      scheduledFlush = yield* Effect.gen(function* () {
        yield* Effect.sleep(STREAM_FLUSH_INTERVAL);
        scheduledFlush = undefined;
        yield* flushPending({ cancelScheduled: false });
      }).pipe(
        Effect.catch((error) =>
          Effect.gen(function* () {
            failed = true;
            yield* Effect.logWarning("agent private stream chunk scheduled flush failed", {
              streamKey,
              error,
            });
          }),
        ),
        Effect.forkDetach({ startImmediately: true }),
      );
    },
  );

  const awaitActiveFlush = Effect.fn("StreamChunks.StreamChunkWriter.awaitActiveFlush")(
    function* (): Effect.fn.Return<void, StreamChunkError> {
      const fiber = activeFlush;
      if (fiber !== undefined) yield* Fiber.join(fiber);
    },
  );

  const flushPending = Effect.fn("StreamChunks.StreamChunkWriter.flushPending")(
    function* (options: {
      readonly cancelScheduled: boolean;
    }): Effect.fn.Return<void, StreamChunkError> {
      if (options.cancelScheduled) yield* cancelScheduledFlush();
      yield* awaitActiveFlush();
      if (failed || pending.length === 0 || pendingPartial === undefined) return;
      const batch = pending;
      const partial = pendingPartial;
      pending = [];
      pendingPartial = undefined;

      const body = serializeStreamEvents(batch, partial);
      const serializedBytes = textEncoder.encode(body).byteLength;
      if (serializedBytes > MAX_STREAM_CHUNK_SEGMENT_BYTES) {
        failed = true;
        return yield* new StreamChunkSegmentTooLarge({
          serializedBytes,
          maximumBytes: MAX_STREAM_CHUNK_SEGMENT_BYTES,
        });
      }

      const flushFiber = yield* store
        .appendStreamChunkSegment(streamKey, segmentIndex++, body)
        .pipe(
          Effect.flatMap((inserted) =>
            inserted ? Effect.void : Effect.sync(() => void (failed = true)),
          ),
          Effect.tapError(() => Effect.sync(() => void (failed = true))),
          Effect.forkDetach({ startImmediately: true }),
        );
      activeFlush = flushFiber;
      yield* Fiber.join(flushFiber).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            if (activeFlush === flushFiber) activeFlush = undefined;
          }),
        ),
      );
      if (active && !failed && pending.length > 0) yield* scheduleFlush();
    },
  );

  const write = Effect.fn("StreamChunks.StreamChunkWriter.write")(function* (
    event: AssistantStreamChunkEvent,
  ): Effect.fn.Return<void, StreamChunkError> {
    if (!active || failed) return;
    pendingPartial = partialFromStreamEvent(event) ?? pendingPartial;
    const compact = compactStreamEvent(event);
    if (compact !== undefined && (compact.type !== "toolcall" || !pending.some(isToolCallMarker))) {
      pending.push(compact);
    }
    yield* scheduleFlush();
  });

  const flush = Effect.fn("StreamChunks.StreamChunkWriter.flush")(function* (): Effect.fn.Return<
    void,
    StreamChunkError
  > {
    yield* flushPending({ cancelScheduled: true });
  });

  const close = Effect.fn("StreamChunks.StreamChunkWriter.close")(function* (): Effect.fn.Return<
    void,
    StreamChunkError
  > {
    active = false;
    yield* flush();
  });

  const cancel = Effect.fn("StreamChunks.StreamChunkWriter.cancel")(
    function* (): Effect.fn.Return<void> {
      active = false;
      pending = [];
      pendingPartial = undefined;
      yield* cancelScheduledFlush();
    },
  );

  return {
    streamKey,
    write,
    flush,
    close,
    cancel,
    isFailed: () => failed,
  } satisfies StreamChunkWriter;
};

type CompactPartial = Omit<AssistantMessage, "role" | "content" | "stopReason" | "errorMessage">;
type CompactStreamEvent =
  | { readonly type: "text_delta"; readonly contentIndex: number; readonly delta: string }
  | { readonly type: "text_end"; readonly contentIndex: number; readonly content: string }
  | { readonly type: "thinking_start"; readonly contentIndex: number }
  | { readonly type: "thinking_delta"; readonly contentIndex: number; readonly delta: string }
  | { readonly type: "thinking_end"; readonly contentIndex: number; readonly content: string }
  | { readonly type: "toolcall" };
type StoredStreamEvent = CompactStreamEvent & { readonly partial?: CompactPartial | undefined };
type ParsedSegmentEvent = StoredStreamEvent | Record<string, unknown>;

const compactStreamEvent = (event: AssistantStreamChunkEvent): CompactStreamEvent | undefined => {
  switch (event.type) {
    case "text_delta":
      return { type: "text_delta", contentIndex: event.contentIndex, delta: event.delta };
    case "text_end":
      return { type: "text_end", contentIndex: event.contentIndex, content: event.content };
    case "thinking_start":
      return { type: "thinking_start", contentIndex: event.contentIndex };
    case "thinking_delta":
      return { type: "thinking_delta", contentIndex: event.contentIndex, delta: event.delta };
    case "thinking_end":
      return { type: "thinking_end", contentIndex: event.contentIndex, content: event.content };
    case "toolcall_start":
    case "toolcall_delta":
    case "toolcall_end":
      return { type: "toolcall" };
    default:
      return undefined;
  }
};

const partialFromStreamEvent = (event: AssistantStreamChunkEvent): AssistantMessage | undefined => {
  if ("partial" in event && event.partial !== undefined) return event.partial;
  if ("message" in event && event.message !== undefined) return event.message;
  if ("error" in event && event.error !== undefined) return event.error;
  return undefined;
};

const isToolCallMarker = (event: CompactStreamEvent): boolean => event.type === "toolcall";

const serializeStreamEvents = (
  pending: ReadonlyArray<CompactStreamEvent>,
  message: AssistantMessage,
): string => {
  const events: StoredStreamEvent[] = [...pending];
  const last = events.at(-1);
  if (last !== undefined) events[events.length - 1] = { ...last, partial: compactPartial(message) };
  return JSON.stringify(events);
};

const compactPartial = (message: AssistantMessage): CompactPartial => {
  const {
    role: _role,
    content: _content,
    stopReason: _stopReason,
    errorMessage: _errorMessage,
    ...partial
  } = message;
  return partial;
};

export const reconstructInterruptedStream = (
  segments: ReadonlyArray<StreamChunkSegment>,
  streamKey: string,
): ReconstructedInterruptedStream | null => {
  const events = segments.flatMap((segment) => parseSegment(segment.body));
  const blocks: Array<AssistantMessage["content"][number] | undefined> = [];
  let partial: AssistantMessage | CompactPartial | undefined;
  let sawToolCall = false;

  for (const update of events) {
    const updatePartial = partialFrom(update);
    if (updatePartial !== undefined) partial = updatePartial;

    if (isToolCallEvent(update)) {
      sawToolCall = true;
      continue;
    }

    const type = update.type;
    if (type === "text_delta") {
      const contentIndex = contentIndexFrom(update);
      const delta = stringField(update, "delta");
      if (contentIndex !== undefined && delta !== undefined)
        appendText(blocks, contentIndex, delta);
    } else if (type === "text_end") {
      const contentIndex = contentIndexFrom(update);
      const content = stringField(update, "content");
      if (contentIndex !== undefined && content !== undefined) {
        blocks[contentIndex] = { type: "text", text: content };
      }
    } else if (type === "thinking_start") {
      const contentIndex = contentIndexFrom(update);
      if (contentIndex !== undefined) blocks[contentIndex] = { type: "thinking", thinking: "" };
    } else if (type === "thinking_delta") {
      const contentIndex = contentIndexFrom(update);
      const delta = stringField(update, "delta");
      if (contentIndex !== undefined && delta !== undefined)
        appendThinking(blocks, contentIndex, delta);
    } else if (type === "thinking_end") {
      const contentIndex = contentIndexFrom(update);
      const content = stringField(update, "content");
      if (contentIndex !== undefined && content !== undefined) {
        blocks[contentIndex] = { type: "thinking", thinking: content };
      }
    }
  }

  if (sawToolCall || partial === undefined) return null;
  const content = blocks.filter((block): block is AssistantMessage["content"][number] => {
    if (block === undefined) return false;
    return block.type === "text"
      ? block.text.length > 0
      : block.type === "thinking" && block.thinking.length > 0;
  });
  if (content.length === 0) return null;

  const recovered: AssistantMessage = {
    ...partial,
    role: "assistant",
    content,
    stopReason: "aborted",
    errorMessage: "Stream interrupted before completion.",
  };

  return {
    partial: recovered,
    interrupted: {
      role: "signal",
      type: "stream_interrupted",
      content: "The previous assistant response was interrupted before completion.",
      attributes: { streamKey },
      timestamp: Date.now(),
    },
    continued: {
      role: "signal",
      type: "stream_continued",
      content:
        "Continue the previous assistant response from exactly where it left off. Do not repeat content already provided.",
      attributes: { streamKey },
      timestamp: Date.now(),
    },
  };
};

const appendText = (
  blocks: Array<AssistantMessage["content"][number] | undefined>,
  contentIndex: number,
  content: string,
): void => {
  const existing = blocks[contentIndex];
  if (existing?.type === "text") existing.text += content;
  else blocks[contentIndex] = { type: "text", text: content };
};

const appendThinking = (
  blocks: Array<AssistantMessage["content"][number] | undefined>,
  contentIndex: number,
  content: string,
): void => {
  const existing = blocks[contentIndex];
  if (existing?.type === "thinking") existing.thinking += content;
  else blocks[contentIndex] = { type: "thinking", thinking: content };
};

const parseSegment = (body: string): ReadonlyArray<ParsedSegmentEvent> => {
  try {
    const parsed = JSON.parse(body) as unknown;
    return Array.isArray(parsed) ? parsed.filter(isRecord) : [];
  } catch {
    return [];
  }
};

const partialFrom = (event: ParsedSegmentEvent): CompactPartial | undefined => {
  const partial = event.partial;
  return isRecord(partial) ? (partial as CompactPartial) : undefined;
};

const isToolCallEvent = (event: ParsedSegmentEvent): boolean =>
  event.type === "toolcall" ||
  event.type === "toolcall_start" ||
  event.type === "toolcall_delta" ||
  event.type === "toolcall_end";

const contentIndexFrom = (event: ParsedSegmentEvent): number | undefined => {
  const value = (event as Record<string, unknown>).contentIndex;
  return typeof value === "number" && Number.isInteger(value) && value >= 0 ? value : undefined;
};

const stringField = (event: ParsedSegmentEvent, field: "content" | "delta"): string | undefined => {
  const value = (event as Record<string, unknown>)[field];
  return typeof value === "string" ? value : undefined;
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

export * as StreamChunks from "./StreamChunks.ts";
