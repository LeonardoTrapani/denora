import type * as Cloudflare from "alchemy/Cloudflare";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { SqlStorage } from "./SqlStorage.ts";

const COMPONENT_PAD = 16;
const ZERO_COMPONENT = "0".repeat(COMPONENT_PAD);

const CREATE_STREAMS_TABLE = `
CREATE TABLE IF NOT EXISTS denora_event_streams (
  path         TEXT PRIMARY KEY,
  next_offset  INTEGER NOT NULL DEFAULT 0,
  closed       INTEGER NOT NULL DEFAULT 0
)`;

const CREATE_ENTRIES_TABLE = `
CREATE TABLE IF NOT EXISTS denora_event_stream_entries (
  path    TEXT NOT NULL,
  seq     INTEGER NOT NULL,
  data    TEXT NOT NULL,
  PRIMARY KEY (path, seq)
)`;

const CREATE_EVENT_KEYS_TABLE = `
CREATE TABLE IF NOT EXISTS denora_event_stream_keys (
  path    TEXT NOT NULL,
  key     TEXT NOT NULL,
  seq     INTEGER NOT NULL,
  data    TEXT NOT NULL,
  PRIMARY KEY (path, key),
  UNIQUE (path, seq)
)`;

const CREATE_EVENT_KEY_TRIGGER = `
CREATE TRIGGER IF NOT EXISTS denora_event_stream_key_append
AFTER INSERT ON denora_event_stream_keys
BEGIN
  INSERT INTO denora_event_stream_entries (path, seq, data)
  VALUES (NEW.path, NEW.seq, NEW.data);
  UPDATE denora_event_streams SET next_offset = next_offset + 1
  WHERE path = NEW.path;
END`;

export const DEFAULT_READ_LIMIT = 100;
export const MAX_READ_LIMIT = 1000;

export class InvalidStreamOffset extends Schema.TaggedErrorClass<InvalidStreamOffset>()(
  "InvalidStreamOffset",
  { offset: Schema.String },
) {}

export class StreamNotFound extends Schema.TaggedErrorClass<StreamNotFound>()("StreamNotFound", {
  path: Schema.String,
}) {}

export class StreamClosed extends Schema.TaggedErrorClass<StreamClosed>()("StreamClosed", {
  path: Schema.String,
}) {}

export class EventSerializationFailed extends Schema.TaggedErrorClass<EventSerializationFailed>()(
  "EventSerializationFailed",
  { cause: Schema.Defect() },
) {}

export class EventStorageFailed extends Schema.TaggedErrorClass<EventStorageFailed>()(
  "EventStorageFailed",
  { operation: Schema.String, cause: Schema.Defect() },
) {}

export type EventStreamError =
  | InvalidStreamOffset
  | StreamNotFound
  | StreamClosed
  | EventSerializationFailed
  | EventStorageFailed;

export interface EventStreamReadResult {
  readonly events: ReadonlyArray<{ readonly data: unknown; readonly offset: string }>;
  readonly nextOffset: string;
  readonly upToDate: boolean;
  readonly closed: boolean;
}

export interface EventStreamMeta {
  readonly nextOffset: string;
  readonly closed: boolean;
}

export interface EventStreamStore {
  readonly createStream: (path: string) => Effect.Effect<void, EventStreamError>;
  readonly appendEvent: (path: string, event: unknown) => Effect.Effect<string, EventStreamError>;
  readonly appendEventOnce: (
    path: string,
    key: string,
    event: unknown,
  ) => Effect.Effect<string, EventStreamError>;
  readonly readEventByKey: (
    path: string,
    key: string,
  ) => Effect.Effect<{ readonly offset: string; readonly event: unknown } | null, EventStreamError>;
  readonly readEvents: (
    path: string,
    opts?: { readonly offset?: string | undefined; readonly limit?: number | undefined },
  ) => Effect.Effect<EventStreamReadResult, EventStreamError>;
  readonly closeStream: (path: string) => Effect.Effect<void, EventStreamError>;
  readonly getStreamMeta: (path: string) => Effect.Effect<EventStreamMeta | null, EventStreamError>;
  readonly subscribe: (path: string, listener: () => void) => Effect.Effect<() => void>;
}

export class Service extends Context.Service<Service, EventStreamStore>()(
  "@denora/server/EventStreamStore",
) {}

export const sqliteLayer: Layer.Layer<Service, EventStorageFailed, SqlStorage.Service> =
  Layer.effect(
    Service,
    Effect.gen(function* () {
      const sql = yield* SqlStorage.Service;
      const store = yield* makeSqliteEventStreamStore(sql);
      return Service.of(store);
    }),
  );

export const formatOffset = (seq: number): string => {
  if (seq === -1) return "-1";
  return `${ZERO_COMPONENT}_${String(seq).padStart(COMPONENT_PAD, "0")}`;
};

export const parseOffset = Effect.fn("EventStreamStore.parseOffset")(function* (
  offset: string,
): Effect.fn.Return<number, InvalidStreamOffset> {
  if (offset === "-1") return -1;
  const match = /^\d+_(\d+)$/.exec(offset);
  const sequence = match?.[1];
  if (sequence === undefined) {
    return yield* new InvalidStreamOffset({ offset });
  }
  return Number.parseInt(sequence, 10);
});

export const runStreamPath = (runId: string): string => `runs/${runId}`;
export const agentStreamPath = (agentName: string, instanceId: string): string =>
  `agents/${agentName}/${instanceId}`;
export const conversationStreamPath = (conversationId: string): string =>
  `conversations/${conversationId}`;

const clampLimit = (limit: number | undefined): number => {
  if (limit === undefined || !Number.isFinite(limit)) return DEFAULT_READ_LIMIT;
  const truncated = Math.trunc(limit);
  if (truncated <= 0) return DEFAULT_READ_LIMIT;
  return Math.min(truncated, MAX_READ_LIMIT);
};

const storageFailure =
  (operation: string) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, EventStorageFailed, R> =>
    effect.pipe(Effect.mapError((cause) => new EventStorageFailed({ operation, cause })));

interface StreamRow extends Record<string, Cloudflare.SqlStorageValue> {
  readonly next_offset: number;
  readonly closed: number;
}

interface EntryRow extends Record<string, Cloudflare.SqlStorageValue> {
  readonly seq: number;
  readonly data: string;
}

interface UpdateRow extends Record<string, Cloudflare.SqlStorageValue> {
  readonly next_offset: number;
}

interface EventKeyRow extends Record<string, Cloudflare.SqlStorageValue> {
  readonly seq: number;
  readonly data: string;
}

const subscribeToPath = (
  listenersByPath: Map<string, Set<() => void>>,
  path: string,
  listener: () => void,
): Effect.Effect<() => void> =>
  Effect.sync(() => {
    let listeners = listenersByPath.get(path);
    if (listeners === undefined) {
      listeners = new Set();
      listenersByPath.set(path, listeners);
    }
    listeners.add(listener);

    return () => {
      listeners.delete(listener);
      if (listeners.size === 0) listenersByPath.delete(path);
    };
  });

const notifyListeners = (listenersByPath: Map<string, Set<() => void>>, path: string): void => {
  const listeners = listenersByPath.get(path);
  if (listeners === undefined) return;
  const snapshot = Array.from(listeners);
  for (const listener of snapshot) {
    try {
      listener();
    } catch {
      // Listener errors are deliberately isolated from append/close writes.
    }
  }
};

export const makeSqliteEventStreamStore = Effect.fn("EventStreamStore.makeSqliteEventStreamStore")(
  function* (sql: Cloudflare.SqlStorage): Effect.fn.Return<EventStreamStore, EventStorageFailed> {
    yield* sql
      .exec(CREATE_STREAMS_TABLE)
      .pipe(storageFailure("create streams table"), Effect.asVoid);
    yield* sql
      .exec(CREATE_ENTRIES_TABLE)
      .pipe(storageFailure("create entries table"), Effect.asVoid);
    yield* sql
      .exec(CREATE_EVENT_KEYS_TABLE)
      .pipe(storageFailure("create event keys table"), Effect.asVoid);
    yield* sql
      .exec(CREATE_EVENT_KEY_TRIGGER)
      .pipe(storageFailure("create event key trigger"), Effect.asVoid);

    const listenersByPath = new Map<string, Set<() => void>>();

    const getStreamMeta = Effect.fn("EventStreamStore.getSqliteStreamMeta")(function* (
      path: string,
    ): Effect.fn.Return<EventStreamMeta | null, EventStorageFailed> {
      const cursor = yield* sql
        .exec<StreamRow>(
          `SELECT next_offset, closed FROM denora_event_streams WHERE path = ?`,
          path,
        )
        .pipe(storageFailure("read stream metadata"));
      const rows = yield* cursor.toArray().pipe(storageFailure("collect stream metadata"));
      const row = rows[0];
      if (row === undefined) return null;
      return { nextOffset: formatOffset(row.next_offset - 1), closed: row.closed === 1 };
    });

    const createStream = (path: string): Effect.Effect<void, EventStorageFailed> =>
      sql
        .exec(`INSERT OR IGNORE INTO denora_event_streams (path) VALUES (?)`, path)
        .pipe(storageFailure("create stream"), Effect.asVoid);

    const appendEvent = Effect.fn("EventStreamStore.appendSqliteEvent")(function* (
      path: string,
      event: unknown,
    ): Effect.fn.Return<string, EventStreamError> {
      const data = yield* Effect.try({
        try: () => JSON.stringify(event),
        catch: (cause) => new EventSerializationFailed({ cause }),
      });

      if (data === undefined) {
        return yield* new EventSerializationFailed({
          cause: new TypeError("Event is not JSON serializable"),
        });
      }

      // Two sequential statements: advance the write cursor, then insert the
      // event at the old cursor position. This mirrors Flue's SQLite store and
      // is safe for the single-process Durable Object SQLite configuration this
      // store targets. A process crash between the two leaves a gap in the
      // sequence, which is harmless for reads because `readEvents` uses
      // `seq > ?` and naturally skips missing numbers. Shared SQLite files
      // across Node processes need a transactional append implementation before
      // being supported. Reference:
      // vendor/flue/packages/runtime/src/runtime/event-stream-store.ts.
      const updateCursor = yield* sql
        .exec<UpdateRow>(
          `UPDATE denora_event_streams
         SET next_offset = next_offset + 1
         WHERE path = ? AND closed = 0
         RETURNING next_offset`,
          path,
        )
        .pipe(storageFailure("advance stream offset"));
      const updated = yield* updateCursor.toArray().pipe(storageFailure("read advanced offset"));
      const updatedRow = updated[0];

      if (updatedRow === undefined) {
        const meta = yield* getStreamMeta(path);
        if (meta === null) return yield* new StreamNotFound({ path });
        return yield* new StreamClosed({ path });
      }

      const seq = updatedRow.next_offset - 1;
      yield* sql
        .exec(
          `INSERT INTO denora_event_stream_entries (path, seq, data) VALUES (?, ?, ?)`,
          path,
          seq,
          data,
        )
        .pipe(storageFailure("insert stream event"), Effect.asVoid);

      notifyListeners(listenersByPath, path);
      return formatOffset(seq);
    });

    const appendEventOnce = Effect.fn("EventStreamStore.appendSqliteEventOnce")(function* (
      path: string,
      key: string,
      event: unknown,
    ): Effect.fn.Return<string, EventStreamError> {
      const data = yield* Effect.try({
        try: () => JSON.stringify(event),
        catch: (cause) => new EventSerializationFailed({ cause }),
      });

      if (data === undefined) {
        return yield* new EventSerializationFailed({
          cause: new TypeError("Event is not JSON serializable"),
        });
      }

      const insertedCursor = yield* sql
        .exec<EventKeyRow>(
          `INSERT OR IGNORE INTO denora_event_stream_keys (path, key, seq, data)
           SELECT path, ?, next_offset, ? FROM denora_event_streams
           WHERE path = ? AND closed = 0
           RETURNING seq, data`,
          key,
          data,
          path,
        )
        .pipe(storageFailure("insert event key"));
      const inserted = yield* insertedCursor
        .toArray()
        .pipe(storageFailure("read inserted event key"));
      const insertedRow = inserted[0];
      if (insertedRow !== undefined) {
        notifyListeners(listenersByPath, path);
        return formatOffset(insertedRow.seq);
      }

      const existingCursor = yield* sql
        .exec<EventKeyRow>(
          `SELECT seq, data FROM denora_event_stream_keys WHERE path = ? AND key = ?`,
          path,
          key,
        )
        .pipe(storageFailure("read existing event key"));
      const existing = yield* existingCursor
        .toArray()
        .pipe(storageFailure("collect existing event key"));
      const existingRow = existing[0];
      if (existingRow !== undefined) {
        if (existingRow.data !== data) {
          return yield* new EventStorageFailed({
            operation: "append event once",
            cause: new Error(`Event key ${key} already has a conflicting payload.`),
          });
        }
        return formatOffset(existingRow.seq);
      }

      const meta = yield* getStreamMeta(path);
      if (meta === null) return yield* new StreamNotFound({ path });
      return yield* new StreamClosed({ path });
    });

    const readEvents = Effect.fn("EventStreamStore.readSqliteEvents")(function* (
      path: string,
      opts?: { readonly offset?: string | undefined; readonly limit?: number | undefined },
    ): Effect.fn.Return<EventStreamReadResult, EventStreamError> {
      const meta = yield* getStreamMeta(path);
      if (meta === null) {
        return { events: [], nextOffset: formatOffset(-1), upToDate: true, closed: false };
      }

      const rawOffset = opts?.offset ?? "-1";
      const limit = clampLimit(opts?.limit);

      let startAfter: number;
      if (rawOffset === "now") {
        return { events: [], nextOffset: meta.nextOffset, upToDate: true, closed: meta.closed };
      }
      startAfter = yield* parseOffset(rawOffset);

      const cursor = yield* sql
        .exec<EntryRow>(
          `SELECT seq, data FROM denora_event_stream_entries
         WHERE path = ? AND seq > ?
         ORDER BY seq ASC
         LIMIT ?`,
          path,
          startAfter,
          limit + 1,
        )
        .pipe(storageFailure("read stream events"));
      const rows = yield* cursor.toArray().pipe(storageFailure("collect stream events"));
      const page = rows.slice(0, limit);

      const events = yield* Effect.forEach(page, (row: EntryRow) =>
        Effect.try({
          try: () => ({ data: JSON.parse(row.data) as unknown, offset: formatOffset(row.seq) }),
          catch: (cause) => new EventStorageFailed({ operation: "parse stream event", cause }),
        }),
      );

      const lastRow = page.at(-1);
      const nextOffset =
        lastRow === undefined ? formatOffset(startAfter) : formatOffset(lastRow.seq);

      return {
        events,
        nextOffset,
        upToDate: rows.length <= limit,
        closed: meta.closed,
      };
    });

    const readEventByKey = Effect.fn("EventStreamStore.readSqliteEventByKey")(function* (
      path: string,
      key: string,
    ): Effect.fn.Return<
      { readonly offset: string; readonly event: unknown } | null,
      EventStreamError
    > {
      const cursor = yield* sql
        .exec<EventKeyRow>(
          `SELECT seq, data FROM denora_event_stream_keys WHERE path = ? AND key = ? LIMIT 1`,
          path,
          key,
        )
        .pipe(storageFailure("read event by key"));
      const rows = yield* cursor.toArray().pipe(storageFailure("collect event by key"));
      const row = rows[0];
      if (row === undefined) return null;
      const event = yield* Effect.try({
        try: () => JSON.parse(row.data) as unknown,
        catch: (cause) => new EventStorageFailed({ operation: "parse keyed event", cause }),
      });
      return { offset: formatOffset(row.seq), event };
    });

    const closeStream = Effect.fn("EventStreamStore.closeSqliteStream")(function* (
      path: string,
    ): Effect.fn.Return<void, EventStorageFailed> {
      yield* sql
        .exec(`UPDATE denora_event_streams SET closed = 1 WHERE path = ?`, path)
        .pipe(storageFailure("close stream"), Effect.asVoid);
      notifyListeners(listenersByPath, path);
    });

    return {
      createStream,
      appendEvent,
      appendEventOnce,
      readEventByKey,
      readEvents,
      closeStream,
      getStreamMeta,
      subscribe: (path, listener) => subscribeToPath(listenersByPath, path, listener),
    } satisfies EventStreamStore;
  },
);

export const makeInMemoryEventStreamStore = (): EventStreamStore => {
  const streams = new Map<string, { nextOffset: number; closed: boolean }>();
  const entries = new Map<string, Array<{ seq: number; data: string }>>();
  const listenersByPath = new Map<string, Set<() => void>>();

  const createStream = (path: string): Effect.Effect<void> =>
    Effect.sync(() => {
      if (streams.has(path)) return;
      streams.set(path, { nextOffset: 0, closed: false });
      entries.set(path, []);
    });

  const getStreamMeta = (path: string): Effect.Effect<EventStreamMeta | null> =>
    Effect.sync(() => {
      const stream = streams.get(path);
      if (stream === undefined) return null;
      return { nextOffset: formatOffset(stream.nextOffset - 1), closed: stream.closed };
    });

  const appendEvent = Effect.fn("EventStreamStore.appendInMemoryEvent")(function* (
    path: string,
    event: unknown,
  ): Effect.fn.Return<string, EventStreamError> {
    const stream = streams.get(path);
    if (stream === undefined) return yield* new StreamNotFound({ path });
    if (stream.closed) return yield* new StreamClosed({ path });
    const data = yield* Effect.try({
      try: () => JSON.stringify(event),
      catch: (cause) => new EventSerializationFailed({ cause }),
    });
    if (data === undefined) {
      return yield* new EventSerializationFailed({
        cause: new TypeError("Event is not JSON serializable"),
      });
    }
    const seq = stream.nextOffset;
    stream.nextOffset += 1;
    entries.get(path)?.push({ seq, data });
    notifyListeners(listenersByPath, path);
    return formatOffset(seq);
  });

  const eventKeys = new Map<string, { seq: number; data: string }>();

  const appendEventOnce = Effect.fn("EventStreamStore.appendInMemoryEventOnce")(function* (
    path: string,
    key: string,
    event: unknown,
  ): Effect.fn.Return<string, EventStreamError> {
    const data = yield* Effect.try({
      try: () => JSON.stringify(event),
      catch: (cause) => new EventSerializationFailed({ cause }),
    });
    if (data === undefined) {
      return yield* new EventSerializationFailed({
        cause: new TypeError("Event is not JSON serializable"),
      });
    }

    const eventKey = `${path}:${key}`;
    const existing = eventKeys.get(eventKey);
    if (existing !== undefined) {
      if (existing.data !== data) {
        return yield* new EventStorageFailed({
          operation: "append event once",
          cause: new Error(`Event key ${key} already has a conflicting payload.`),
        });
      }
      return formatOffset(existing.seq);
    }

    const stream = streams.get(path);
    if (stream === undefined) return yield* new StreamNotFound({ path });
    if (stream.closed) return yield* new StreamClosed({ path });
    const seq = stream.nextOffset;
    stream.nextOffset += 1;
    eventKeys.set(eventKey, { seq, data });
    entries.get(path)?.push({ seq, data });
    notifyListeners(listenersByPath, path);
    return formatOffset(seq);
  });

  const readEvents = Effect.fn("EventStreamStore.readInMemoryEvents")(function* (
    path: string,
    opts?: { readonly offset?: string | undefined; readonly limit?: number | undefined },
  ): Effect.fn.Return<EventStreamReadResult, EventStreamError> {
    const meta = yield* getStreamMeta(path);
    if (meta === null) {
      return { events: [], nextOffset: formatOffset(-1), upToDate: true, closed: false };
    }
    const rawOffset = opts?.offset ?? "-1";
    const limit = clampLimit(opts?.limit);
    if (rawOffset === "now") {
      return { events: [], nextOffset: meta.nextOffset, upToDate: true, closed: meta.closed };
    }
    const startAfter = yield* parseOffset(rawOffset);
    const rows = (entries.get(path) ?? []).filter((entry) => entry.seq > startAfter);
    const page = rows.slice(0, limit);
    const events = yield* Effect.forEach(page, (entry) =>
      Effect.try({
        try: () => ({ data: JSON.parse(entry.data) as unknown, offset: formatOffset(entry.seq) }),
        catch: (cause) => new EventStorageFailed({ operation: "parse stream event", cause }),
      }),
    );
    const lastRow = page.at(-1);
    return {
      events,
      nextOffset: lastRow === undefined ? formatOffset(startAfter) : formatOffset(lastRow.seq),
      upToDate: rows.length <= limit,
      closed: meta.closed,
    };
  });

  const readEventByKey = Effect.fn("EventStreamStore.readInMemoryEventByKey")(function* (
    path: string,
    key: string,
  ): Effect.fn.Return<
    { readonly offset: string; readonly event: unknown } | null,
    EventStreamError
  > {
    const existing = eventKeys.get(`${path}:${key}`);
    if (existing === undefined) return null;
    const event = yield* Effect.try({
      try: () => JSON.parse(existing.data) as unknown,
      catch: (cause) => new EventStorageFailed({ operation: "parse keyed event", cause }),
    });
    return { offset: formatOffset(existing.seq), event };
  });

  const closeStream = (path: string): Effect.Effect<void> =>
    Effect.sync(() => {
      const stream = streams.get(path);
      if (stream !== undefined) stream.closed = true;
      notifyListeners(listenersByPath, path);
    });

  return {
    createStream,
    appendEvent,
    appendEventOnce,
    readEventByKey,
    readEvents,
    closeStream,
    getStreamMeta,
    subscribe: (path, listener) => subscribeToPath(listenersByPath, path, listener),
  } satisfies EventStreamStore;
};

export * as EventStreamStore from "./EventStreamStore.ts";
