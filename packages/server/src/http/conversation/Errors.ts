import * as Schema from "effect/Schema";

export class ConversationRequestFailed extends Schema.TaggedErrorClass<ConversationRequestFailed>()(
  "ConversationRequestFailed",
  {
    reason: Schema.Literals([
      "invalid_stream_offset",
      "stream_not_found",
      "stream_closed",
      "event_serialization_failed",
      "event_storage_failed",
      "persistence_failed",
      "conversation_not_authorized",
      "conversation_not_active",
    ]),
    message: Schema.String,
  },
  { httpApiStatus: 500 },
) {}

export * as ConversationErrors from "./Errors.ts";
