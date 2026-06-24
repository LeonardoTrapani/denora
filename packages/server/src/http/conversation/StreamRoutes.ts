import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Result from "effect/Result";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import { Auth } from "../../auth/Auth.ts";
import { CurrentUser } from "../../auth/User.ts";
import { Conversations } from "../../conversation/Conversations.ts";
import { methodNotAllowedResponse, unauthorizedResponse } from "../../agent-run/StreamProtocol.ts";

const streamAuthorizationLayer = HttpRouter.middleware<{ provides: CurrentUser }>()(
  Effect.gen(function* () {
    const auth = yield* Auth.Service;

    return (httpEffect) =>
      Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        const webRequest = yield* HttpServerRequest.toWeb(request);
        const user = yield* auth.requireSession(webRequest).pipe(Effect.result);
        if (Result.isFailure(user)) {
          return HttpServerResponse.fromWeb(unauthorizedResponse());
        }

        return yield* Effect.provideService(httpEffect, CurrentUser, user.success);
      });
  }),
).layer;

export const routes = HttpRouter.use((router) =>
  Effect.gen(function* () {
    const conversations = yield* Conversations.Service;

    yield* router.add(
      "*",
      "/conversations/:conversationId/events",
      (request) => streamRoute(request, conversations),
      {
        uninterruptible: false,
      },
    );
  }),
).pipe(Layer.provide(streamAuthorizationLayer));

const streamRoute = (
  request: HttpServerRequest.HttpServerRequest,
  conversations: Conversations.Interface,
) =>
  Effect.gen(function* () {
    const params = yield* HttpRouter.params;
    const conversationId = params.conversationId;
    if (conversationId === undefined || conversationId.length === 0) {
      return HttpServerResponse.fromWeb(
        new Response(
          JSON.stringify({
            error: { type: "invalid_request", message: "Conversation id is required." },
          }),
          {
            status: 400,
            headers: { "content-type": "application/json" },
          },
        ),
      );
    }

    if (request.method !== "GET" && request.method !== "HEAD") {
      return HttpServerResponse.fromWeb(methodNotAllowedResponse());
    }

    const user = yield* CurrentUser;
    return yield* conversations.streamRequest("default", conversationId, user.id, request);
  });

export * as ConversationStreamRoutes from "./StreamRoutes.ts";
