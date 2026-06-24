import type { AgentMessage } from "@earendil-works/pi-agent-core";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

type UserAgentMessage = Extract<AgentMessage, { readonly role: "user" }>;

export const ConversationId = Schema.String.check(Schema.isStartsWith("conversation_")).pipe(
  Schema.brand("ConversationId"),
);
export type ConversationId = typeof ConversationId.Type;

export const MessageId = Schema.String.check(Schema.isStartsWith("message_")).pipe(
  Schema.brand("MessageId"),
);
export type MessageId = typeof MessageId.Type;

export const SubmissionId = Schema.String.check(Schema.isStartsWith("submission_")).pipe(
  Schema.brand("SubmissionId"),
);
export type SubmissionId = typeof SubmissionId.Type;

export const RunId = Schema.String.check(Schema.isStartsWith("run_")).pipe(Schema.brand("RunId"));
export type RunId = typeof RunId.Type;

export const UserId = Schema.String.check(Schema.isMinLength(1)).pipe(Schema.brand("UserId"));
export type UserId = typeof UserId.Type;

export const AgentName = Schema.String.check(Schema.isMinLength(1)).pipe(Schema.brand("AgentName"));
export type AgentName = typeof AgentName.Type;

export const ImageContent = Schema.Struct({
  type: Schema.Literal("image"),
  data: Schema.String,
  mimeType: Schema.String,
});
export type ImageContent = typeof ImageContent.Type;

export const TextContent = Schema.Struct({ text: Schema.String });
export type TextContent = typeof TextContent.Type;

const RichUserContent = Schema.Struct({
  text: Schema.optionalKey(Schema.String),
  image: Schema.optionalKey(Schema.Union([ImageContent, Schema.Array(ImageContent)])),
  images: Schema.optionalKey(Schema.Array(ImageContent)),
});

const AssistantResult = Schema.Struct({ assistantText: Schema.String });
const SubmittedRunInput = Schema.Struct({
  prompt: Schema.optionalKey(Schema.String),
  submittedMessage: Schema.optionalKey(Schema.Unknown),
});
const StreamEventIndex = Schema.Struct({ eventIndex: Schema.Number });
const StreamEventTimestamp = Schema.Struct({ timestamp: Schema.String });

export const makeConversationId = (): ConversationId =>
  Schema.decodeUnknownSync(ConversationId)(generatedId("conversation"));
export const makeMessageId = (): MessageId =>
  Schema.decodeUnknownSync(MessageId)(generatedId("message"));
export const makeSubmissionId = (): SubmissionId =>
  Schema.decodeUnknownSync(SubmissionId)(generatedId("submission"));
export const makeRunId = (): RunId => Schema.decodeUnknownSync(RunId)(generatedId("run"));

export const promptFromContent = (content: unknown): string => {
  const text = Schema.decodeUnknownOption(Schema.String)(content);
  if (Option.isSome(text)) return text.value;

  const structured = Schema.decodeUnknownOption(TextContent)(content);
  if (Option.isSome(structured)) return structured.value.text;

  return JSON.stringify(content) ?? "";
};

export const promptFromInput = (input: unknown): string => {
  const text = Schema.decodeUnknownOption(Schema.String)(input);
  if (Option.isSome(text)) return text.value;

  const decoded = Schema.decodeUnknownOption(SubmittedRunInput)(input);
  return Option.isSome(decoded) ? (decoded.value.prompt ?? "") : "";
};

export const submittedContentFromInput = (input: unknown): unknown => {
  const decoded = Schema.decodeUnknownOption(SubmittedRunInput)(input);
  if (Option.isSome(decoded) && decoded.value.submittedMessage !== undefined) {
    return messageContentFromSubmitted(decoded.value.submittedMessage);
  }
  return promptFromInput(input);
};

export const messageContentFromSubmitted = (submitted: unknown): unknown => {
  const rich = richUserMessage(submitted);
  return rich === undefined ? promptFromContent(submitted) : rich.content;
};

export const richUserMessage = (
  content: unknown,
  timestamp = Date.now(),
): UserAgentMessage | undefined => {
  const decoded = Schema.decodeUnknownOption(RichUserContent)(content);
  if (Option.isNone(decoded)) return undefined;

  const images = imageContents(decoded.value);
  if (images.length === 0) return undefined;

  const textContent = Option.match(Option.fromUndefinedOr(decoded.value.text), {
    onNone: () => [],
    onSome: (text) => [{ type: "text" as const, text }],
  });

  return {
    role: "user",
    content: [...textContent, ...images],
    timestamp,
  };
};

export const assistantTextFromResult = (result: unknown): string => {
  const decoded = Schema.decodeUnknownOption(AssistantResult)(result);
  return Option.isSome(decoded) ? decoded.value.assistantText : "";
};

export const nextEventIndex = (event: unknown): number => {
  const decoded = Schema.decodeUnknownOption(StreamEventIndex)(event);
  return Option.isSome(decoded) ? decoded.value.eventIndex + 1 : 0;
};

export const eventIndexFrom = (event: unknown): number | undefined => {
  const decoded = Schema.decodeUnknownOption(StreamEventIndex)(event);
  return Option.isSome(decoded) ? decoded.value.eventIndex : undefined;
};

export const timestampFrom = (event: unknown): string | undefined => {
  const decoded = Schema.decodeUnknownOption(StreamEventTimestamp)(event);
  return Option.isSome(decoded) ? decoded.value.timestamp : undefined;
};

const imageContents = (content: typeof RichUserContent.Type): ReadonlyArray<ImageContent> => [
  ...optionalImageContents(content.image),
  ...(content.images ?? []),
];

const optionalImageContents = (value: unknown): ReadonlyArray<ImageContent> => {
  const image = Schema.decodeUnknownOption(ImageContent)(value);
  if (Option.isSome(image)) return [image.value];

  const images = Schema.decodeUnknownOption(Schema.Array(ImageContent))(value);
  return Option.isSome(images) ? images.value : [];
};

const generatedId = (prefix: string): string => `${prefix}_${crypto.randomUUID()}`;

export * as ConversationDomain from "./ConversationDomain.ts";
