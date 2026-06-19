import { WebConfig } from "./WebConfig.ts";

export interface StreamAgentMessageInput {
  readonly agentId: string;
  readonly threadId: string;
  readonly message: string;
  readonly signal?: AbortSignal | undefined;
  readonly onChunk: (chunk: string) => void;
}

export async function streamAgentMessage(input: StreamAgentMessageInput): Promise<void> {
  const streamId = crypto.randomUUID();
  const init: RequestInit = {
    body: JSON.stringify({ message: input.message }),
    credentials: "include",
    headers: { "content-type": "application/json" },
    method: "POST",
  };

  if (input.signal !== undefined) {
    init.signal = input.signal;
  }

  console.info("[agent-stream] request start", {
    agentId: input.agentId,
    messageLength: input.message.length,
    streamId,
    threadId: input.threadId,
  });

  const response = await fetch(
    `${WebConfig.requireApiUrl()}/agents/${encodeURIComponent(input.agentId)}/threads/${encodeURIComponent(input.threadId)}/messages/stream`,
    init,
  );

  console.info("[agent-stream] response headers", {
    contentType: response.headers.get("content-type"),
    ok: response.ok,
    status: response.status,
    streamId,
  });

  if (!response.ok) {
    const detail = await response.text();
    console.error("[agent-stream] response failed", { detail, status: response.status, streamId });
    throw new Error(detail.length > 0 ? detail : `Stream request failed with ${response.status}`);
  }

  if (response.body === null) {
    console.error("[agent-stream] missing response body", { streamId });
    throw new Error("The stream response did not include a body");
  }

  const reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
  let chunks = 0;

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        console.info("[agent-stream] stream complete", { chunks, streamId });
        return;
      }
      chunks += 1;
      console.info("[agent-stream] chunk", {
        chunkLength: value.length,
        chunks,
        streamId,
      });
      input.onChunk(value);
    }
  } catch (error) {
    console.error("[agent-stream] reader failed", { chunks, error, streamId });
    throw error;
  } finally {
    reader.releaseLock();
  }
}

export * as AgentStream from "./agent-stream.ts";
