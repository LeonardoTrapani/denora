import * as Schema from "effect/Schema";

export class SendMessageRequest extends Schema.Class<SendMessageRequest>("SendMessageRequest")({
  message: Schema.String,
}) {}

export class AgentMessageResponse extends Schema.Class<AgentMessageResponse>(
  "AgentMessageResponse",
)({
  threadId: Schema.String,
  agentId: Schema.String,
  role: Schema.Literal("assistant"),
  content: Schema.String,
}) {}

export class AgentThreadError extends Schema.TaggedErrorClass<AgentThreadError>()(
  "AgentThreadError",
  {
    operation: Schema.Literals(["send", "stream"]),
    message: Schema.String,
    model: Schema.optional(Schema.String),
    detail: Schema.optional(Schema.String),
  },
  { httpApiStatus: 500 },
) {}

export interface ThreadReply {
  readonly content: string;
}

export * as AgentSchema from "./Schema.ts";
