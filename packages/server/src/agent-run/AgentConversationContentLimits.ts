export const MAX_AGENT_CONVERSATION_TEXT_LENGTH = 256 * 1024;
export const MAX_AGENT_CONVERSATION_IMAGE_DATA_LENGTH = 256 * 1024;
export const MAX_AGENT_CONVERSATION_JSON_LENGTH = 512 * 1024;

export const assertAgentConversationContentWithinLimits = (value: unknown): void => {
  visitContent(value, new WeakSet<object>());
};

export const assertAgentConversationJsonWithinLimits = (json: string, label: string): void => {
  if (json.length > MAX_AGENT_CONVERSATION_JSON_LENGTH) {
    throw new Error(
      `[denora] ${label} exceeds the ${MAX_AGENT_CONVERSATION_JSON_LENGTH} character serialized limit.`,
    );
  }
};

const visitContent = (value: unknown, seen: WeakSet<object>): void => {
  if (typeof value === "string") {
    assertTextWithinLimit(value);
    return;
  }
  if (value === null || typeof value !== "object") return;
  if (seen.has(value)) return;
  seen.add(value);

  if (Array.isArray(value)) {
    for (const item of value) visitContent(item, seen);
    return;
  }

  const record = value as Record<string, unknown>;
  const imageDataChecked = record.type === "image" && typeof record.data === "string";
  if (imageDataChecked) assertImageDataWithinLimit(record.data as string);

  for (const [key, child] of Object.entries(record)) {
    if (imageDataChecked && key === "data") continue;
    visitContent(child, seen);
  }
};

const assertTextWithinLimit = (text: string): void => {
  if (text.length > MAX_AGENT_CONVERSATION_TEXT_LENGTH) {
    throw new Error(
      `[denora] Conversation text content exceeds the ${MAX_AGENT_CONVERSATION_TEXT_LENGTH} character limit.`,
    );
  }
};

const assertImageDataWithinLimit = (data: string): void => {
  if (data.length > MAX_AGENT_CONVERSATION_IMAGE_DATA_LENGTH) {
    throw new Error(
      `[denora] Conversation image data exceeds the ${MAX_AGENT_CONVERSATION_IMAGE_DATA_LENGTH} character limit.`,
    );
  }
};

export * as AgentConversationContentLimits from "./AgentConversationContentLimits.ts";
