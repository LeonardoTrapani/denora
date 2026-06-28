import { describe, expect, it } from "vitest";

import { emptyChatState, reduceChatEvent } from "./reducer.ts";
import type { DenoraConversationEvent } from "./types.ts";

const base = {
  v: 3 as const,
  instanceId: "conversation-1",
  agentName: "denora",
  timestamp: "2026-06-12T00:00:00.000Z",
};

function event(value: Record<string, unknown>): DenoraConversationEvent {
  return { ...base, ...value } as DenoraConversationEvent;
}

function message(
  type: "message_start" | "message_end",
  value: Record<string, unknown>,
  extra: Record<string, unknown> = {},
): DenoraConversationEvent {
  return event({ type, message: value, eventIndex: 1, ...extra });
}

describe("reduceChatEvent()", () => {
  it("keeps an empty assistant start empty instead of rendering JSON []", () => {
    const state = reduceChatEvent(
      emptyChatState,
      message("message_start", { role: "assistant", content: [] }, { turnId: "turn-1" }),
    );

    expect(state.messages).toEqual([
      {
        id: "turn:turn-1",
        role: "assistant",
        metadata: undefined,
        parts: [],
      },
    ]);
  });

  it("builds text and thinking parts from ordered deltas when a message has started", () => {
    let state = reduceChatEvent(
      emptyChatState,
      message("message_start", { role: "assistant", content: [] }, { turnId: "turn-1" }),
    );
    state = reduceChatEvent(
      state,
      event({ type: "thinking_start", contentIndex: 0, eventIndex: 2, turnId: "turn-1" }),
    );
    state = reduceChatEvent(
      state,
      event({
        type: "thinking_delta",
        contentIndex: 0,
        delta: "consider",
        eventIndex: 3,
        turnId: "turn-1",
      }),
    );
    state = reduceChatEvent(
      state,
      event({
        type: "thinking_end",
        contentIndex: 0,
        content: "consider carefully",
        eventIndex: 4,
        turnId: "turn-1",
      }),
    );
    state = reduceChatEvent(
      state,
      event({ type: "text_delta", text: "hello", eventIndex: 5, turnId: "turn-1" }),
    );
    state = reduceChatEvent(
      state,
      event({ type: "text_delta", text: " world", eventIndex: 6, turnId: "turn-1" }),
    );

    expect(state.messages).toEqual([
      {
        id: "turn:turn-1",
        role: "assistant",
        metadata: undefined,
        parts: [
          { type: "reasoning", text: "consider carefully", state: "done" },
          { type: "text", text: "hello world", state: "streaming" },
        ],
      },
    ]);
  });

  it("correlates interleaved thinking events by content index", () => {
    let state = reduceChatEvent(
      emptyChatState,
      message("message_start", { role: "assistant", content: [] }, { turnId: "turn-1" }),
    );
    state = reduceChatEvent(
      state,
      event({ type: "thinking_start", contentIndex: 0, eventIndex: 2, turnId: "turn-1" }),
    );
    state = reduceChatEvent(
      state,
      event({ type: "thinking_start", contentIndex: 2, eventIndex: 3, turnId: "turn-1" }),
    );
    state = reduceChatEvent(
      state,
      event({
        type: "thinking_delta",
        contentIndex: 0,
        delta: "first",
        eventIndex: 4,
        turnId: "turn-1",
      }),
    );
    state = reduceChatEvent(
      state,
      event({
        type: "thinking_end",
        contentIndex: 0,
        content: "first done",
        eventIndex: 5,
        turnId: "turn-1",
      }),
    );
    state = reduceChatEvent(
      state,
      event({
        type: "thinking_delta",
        contentIndex: 2,
        delta: "second",
        eventIndex: 6,
        turnId: "turn-1",
      }),
    );

    expect(state.messages[0]?.parts).toEqual([
      { type: "reasoning", text: "first done", state: "done" },
      { type: "reasoning", text: "second", state: "streaming" },
    ]);

    state = reduceChatEvent(
      state,
      message(
        "message_end",
        {
          role: "assistant",
          content: [
            { type: "thinking", thinking: "first final" },
            { type: "text", text: "answer" },
            { type: "thinking", thinking: "second final" },
          ],
        },
        { turnId: "turn-1", eventIndex: 7 },
      ),
    );
    state = reduceChatEvent(
      state,
      event({
        type: "thinking_delta",
        contentIndex: 0,
        delta: " stale",
        eventIndex: 8,
        turnId: "turn-1",
      }),
    );

    expect(state.messages[0]?.parts).toEqual([
      { type: "reasoning", text: "first final", state: "done" },
      { type: "text", text: "answer", state: "done" },
      { type: "reasoning", text: "second final", state: "done" },
    ]);
  });

  it("does not duplicate provisional parts when an interrupted partial batch is replayed", () => {
    const events = [
      message("message_start", { role: "assistant", content: [] }, { turnId: "turn-1" }),
      event({ type: "thinking_start", eventIndex: 2, turnId: "turn-1" }),
      event({ type: "thinking_delta", delta: "checking", eventIndex: 3, turnId: "turn-1" }),
      event({ type: "text_delta", text: "partial", eventIndex: 4, turnId: "turn-1" }),
      event({
        type: "tool_start",
        toolName: "search",
        toolCallId: "tool-1",
        eventIndex: 5,
        turnId: "turn-1",
      }),
    ];
    const once = events.reduce(reduceChatEvent, emptyChatState);
    const replayed = events.reduce(reduceChatEvent, once);

    expect(replayed.messages).toEqual(once.messages);
    expect(replayed.messages[0]?.parts).toEqual([
      { type: "reasoning", text: "checking", state: "streaming" },
      { type: "text", text: "partial", state: "streaming" },
      {
        type: "dynamic-tool",
        toolName: "search",
        toolCallId: "tool-1",
        state: "input-available",
        input: undefined,
      },
    ]);
  });

  it("reconciles streamed content to the authoritative terminal message", () => {
    let state = reduceChatEvent(
      emptyChatState,
      message("message_start", { role: "assistant", content: [] }, { turnId: "turn-1" }),
    );
    state = reduceChatEvent(
      state,
      event({ type: "text_delta", text: "draft", eventIndex: 2, turnId: "turn-1" }),
    );
    state = reduceChatEvent(
      state,
      message(
        "message_end",
        { role: "assistant", content: [{ type: "text", text: "final" }] },
        { turnId: "turn-1", eventIndex: 3 },
      ),
    );

    expect(state.messages[0]?.parts).toEqual([{ type: "text", text: "final", state: "done" }]);
  });

  it("provisions an assistant message when a late stream begins at tool_start", () => {
    const state = reduceChatEvent(
      emptyChatState,
      event({
        type: "tool_start",
        toolName: "search",
        toolCallId: "tool-1",
        eventIndex: 20,
        turnId: "turn-9",
      }),
    );

    expect(state.messages).toEqual([
      {
        id: "turn:turn-9",
        role: "assistant",
        parts: [
          {
            type: "dynamic-tool",
            toolName: "search",
            toolCallId: "tool-1",
            state: "input-available",
            input: undefined,
          },
        ],
      },
    ]);
  });

  it("reconciles receipt-before-echo without matching message text", () => {
    let state = reduceChatEvent(emptyChatState, {
      type: "local_send_submitted",
      localId: "local-1",
      content: { text: "same" },
    });
    state = reduceChatEvent(state, {
      type: "local_send_admitted",
      localId: "local-1",
      submissionId: "submission-1",
    });
    state = reduceChatEvent(
      state,
      message(
        "message_end",
        { role: "user", content: "same" },
        {
          submissionId: "submission-1",
          turnId: "submission:submission-1:user",
        },
      ),
    );

    expect(state.messages).toHaveLength(1);
    expect(state.messages[0]?.id).toBe("submission:submission-1:user:0");
  });

  it("keeps an optimistic user message position when its durable echo arrives late", () => {
    let state = reduceChatEvent(emptyChatState, {
      type: "local_send_submitted",
      localId: "local-1",
      content: { text: "hello" },
    });
    state = reduceChatEvent(state, {
      type: "local_send_admitted",
      localId: "local-1",
      submissionId: "submission-1",
    });
    state = reduceChatEvent(
      state,
      message(
        "message_end",
        { role: "assistant", content: [{ type: "text", text: "reply" }] },
        { submissionId: "submission-1", turnId: "turn-1", eventIndex: 2 },
      ),
    );
    state = reduceChatEvent(
      state,
      message(
        "message_end",
        { role: "user", content: "hello" },
        {
          submissionId: "submission-1",
          turnId: "submission:submission-1:user",
          eventIndex: 3,
        },
      ),
    );

    expect(state.messages.map((item) => item.id)).toEqual([
      "submission:submission-1:user:0",
      "turn:turn-1",
    ]);
  });

  it("ignores historical failed settled submissions that are not local pending sends", () => {
    const state = reduceChatEvent(
      emptyChatState,
      event({
        type: "submission_settled",
        eventIndex: 10,
        submissionId: "old-submission",
        outcome: "failed",
        error: { message: "old failure" },
      }),
    );

    expect(state.status).toBe("idle");
    expect(state.error).toBeUndefined();
    expect(state.settledSubmissionIds).toEqual([]);
  });

  it("uses Flue fallback identity when no turn or submission id is available", () => {
    const state = reduceChatEvent(
      emptyChatState,
      message(
        "message_end",
        { role: "assistant", content: [{ type: "text", text: "done" }] },
        { eventIndex: 42, timestamp: "2026-06-12T00:01:00.000Z" },
      ),
    );

    expect(state.messages[0]?.id).toBe("event:2026-06-12T00:01:00.000Z:42:assistant");
  });
});
