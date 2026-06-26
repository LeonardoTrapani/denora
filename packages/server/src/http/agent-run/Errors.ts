import * as Schema from "effect/Schema";

export class CreateAgentRunFailed extends Schema.TaggedErrorClass<CreateAgentRunFailed>()(
  "CreateAgentRunFailed",
  {
    reason: Schema.Literals([
      "invalid_stream_offset",
      "stream_not_found",
      "stream_closed",
      "event_serialization_failed",
      "event_storage_failed",
      "persistence_failed",
      "run_not_authorized",
    ]),
    message: Schema.String,
  },
  { httpApiStatus: 500 },
) {}

export * as AgentRunErrors from "./Errors.ts";
