import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import { AuthorizationApi } from "../../auth/AuthorizationApi.ts";
import { Conversations } from "../../conversation/Conversations.ts";
import { DenoraApi } from "../Api.ts";
import {
  AbortConversationResponse,
  Conversation,
  ConversationMessage,
  SubmitConversationMessageResponse,
} from "./Api.ts";

export const layer = HttpApiBuilder.group(DenoraApi, "Conversation", (handlers) =>
  handlers
    .handle("createConversation", ({ payload }) =>
      Effect.gen(function* () {
        const user = yield* AuthorizationApi.CurrentUser;
        const conversations = yield* Conversations.Service;
        const created = yield* conversations.createConversation({
          userId: user.id,
          conversationId: payload.conversationId,
          agentId: payload.agentId,
          title: payload.title,
          metadata: payload.metadata,
        });
        return new Conversation(created);
      }),
    )
    .handle("listConversations", () =>
      Effect.gen(function* () {
        const user = yield* AuthorizationApi.CurrentUser;
        const conversations = yield* Conversations.Service;
        const rows = yield* conversations.listConversations(user.id);
        return rows.map((row) => new Conversation(row));
      }),
    )
    .handle("listConversationMessages", ({ params }) =>
      Effect.gen(function* () {
        const user = yield* AuthorizationApi.CurrentUser;
        const conversations = yield* Conversations.Service;
        const rows = yield* conversations.listMessages({
          conversationId: params.conversationId,
          userId: user.id,
        });
        return rows.map((row) => new ConversationMessage(row));
      }),
    )
    .handle("submitConversationMessage", ({ params, payload }) =>
      Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        const user = yield* AuthorizationApi.CurrentUser;
        const conversations = yield* Conversations.Service;
        const submitted = yield* conversations.submitMessage({
          conversationId: params.conversationId,
          userId: user.id,
          message: payload.message,
          content: payload.content,
        });
        const body = new SubmitConversationMessageResponse({
          conversationId: submitted.conversationId,
          messageId: submitted.messageId,
          submissionId: submitted.submissionId,
          runId: submitted.runId,
          streamPath: submitted.streamPath,
          streamUrl: streamUrl(request, submitted.conversationId),
          offset: submitted.offset,
        });
        return HttpServerResponse.fromWeb(
          new Response(JSON.stringify(body), {
            status: 202,
            headers: {
              "content-type": "application/json",
              Location: body.streamUrl,
              "Stream-Next-Offset": body.offset,
            },
          }),
        );
      }),
    )
    .handle("abortConversation", ({ params, payload }) =>
      Effect.gen(function* () {
        const user = yield* AuthorizationApi.CurrentUser;
        const conversations = yield* Conversations.Service;
        const result = yield* conversations.abortConversation({
          conversationId: params.conversationId,
          userId: user.id,
          reason: payload.reason,
        });
        return new AbortConversationResponse({ abortedSubmissions: result.abortedSubmissions });
      }),
    )
    .handle("archiveConversation", ({ params }) =>
      Effect.gen(function* () {
        const user = yield* AuthorizationApi.CurrentUser;
        const conversations = yield* Conversations.Service;
        const archived = yield* conversations.archiveConversation({
          conversationId: params.conversationId,
          userId: user.id,
        });
        return new Conversation(archived);
      }),
    )
    .handle("deleteConversation", ({ params }) =>
      Effect.gen(function* () {
        const user = yield* AuthorizationApi.CurrentUser;
        const conversations = yield* Conversations.Service;
        const deleted = yield* conversations.deleteConversation({
          conversationId: params.conversationId,
          userId: user.id,
        });
        return new Conversation(deleted);
      }),
    ),
);

const streamUrl = (
  request: HttpServerRequest.HttpServerRequest,
  conversationId: string,
): string => {
  const path = `/conversations/${encodeURIComponent(conversationId)}/events`;
  const url = HttpServerRequest.toURL(request);
  if (Option.isNone(url)) return path;
  const next = new URL(url.value.toString());
  next.pathname = path;
  next.search = "";
  return next.toString();
};

export * as ConversationHandlers from "./Handlers.ts";
