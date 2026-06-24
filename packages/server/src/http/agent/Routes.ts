import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Result from "effect/Result";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import { Auth } from "../../auth/Auth.ts";
import { CurrentUser } from "../../auth/User.ts";
import { methodNotAllowedResponse, unauthorizedResponse } from "../../agent-run/StreamProtocol.ts";
import { Conversations } from "../../conversation/Conversations.ts";

const authorizationLayer = HttpRouter.middleware<{ provides: CurrentUser }>()(
  Effect.gen(function* () {
    const auth = yield* Auth.Service;

    return (httpEffect) =>
      Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        const webRequest = yield* HttpServerRequest.toWeb(request);
        const user = yield* auth.requireSession(webRequest).pipe(Effect.result);
        if (Result.isFailure(user)) return HttpServerResponse.fromWeb(unauthorizedResponse());

        return yield* Effect.provideService(httpEffect, CurrentUser, user.success);
      });
  }),
).layer;

export const routes = HttpRouter.use((router) =>
  Effect.gen(function* () {
    const conversations = yield* Conversations.Service;

    yield* router.add("*", "/agents/:name/:id", (request) => agentRoute(request, conversations), {
      uninterruptible: false,
    });
  }),
).pipe(Layer.provide(authorizationLayer));

const agentRoute = (
  request: HttpServerRequest.HttpServerRequest,
  conversations: Conversations.Interface,
) =>
  Effect.gen(function* () {
    const params = yield* HttpRouter.params;
    const agentName = params.name;
    const instanceId = params.id;
    if (
      agentName === undefined ||
      agentName.length === 0 ||
      instanceId === undefined ||
      instanceId.length === 0
    ) {
      return HttpServerResponse.fromWeb(
        jsonResponse(
          {
            error: { type: "invalid_request", message: "Agent name and instance id are required." },
          },
          400,
        ),
      );
    }

    const user = yield* CurrentUser;
    if (request.method === "GET" || request.method === "HEAD") {
      return yield* conversations.streamRequest(agentName, instanceId, user.id, request);
    }
    if (request.method !== "POST") return HttpServerResponse.fromWeb(methodNotAllowedResponse());

    const payloadResult = yield* parseDirectAgentPayload(request).pipe(Effect.result);
    if (Result.isFailure(payloadResult)) return HttpServerResponse.fromWeb(payloadResult.failure);
    const payload = payloadResult.success;
    const webRequest = yield* HttpServerRequest.toWeb(request);
    const waitForResult = new URL(webRequest.url).searchParams.get("wait") === "result";
    const submitted = yield* conversations.submitMessage({
      conversationId: instanceId,
      userId: user.id,
      agentName,
      message: payload.message,
      images: payload.images,
      waitForResult,
    });
    const body = {
      streamUrl: streamUrl(webRequest),
      offset: submitted.offset,
      submissionId: submitted.submissionId,
      ...(waitForResult ? { result: submitted.result ?? null } : {}),
    };
    return HttpServerResponse.fromWeb(
      new Response(JSON.stringify(body), {
        status: waitForResult ? 200 : 202,
        headers: waitForResult
          ? { "content-type": "application/json" }
          : {
              "content-type": "application/json",
              Location: body.streamUrl,
              "Stream-Next-Offset": submitted.offset,
            },
      }),
    );
  });

interface DirectAgentPayload {
  readonly message: string;
  readonly images?: ReadonlyArray<{
    readonly type: "image";
    readonly data: string;
    readonly mimeType: string;
  }>;
}

const parseDirectAgentPayload = (
  request: HttpServerRequest.HttpServerRequest,
): Effect.Effect<DirectAgentPayload, Response> =>
  request.json.pipe(
    Effect.mapError(() => invalidPayloadResponse()),
    Effect.flatMap((value) =>
      Effect.try({
        try: () => parseDirectAgentPayloadJson(value),
        catch: () => invalidPayloadResponse(),
      }),
    ),
  );

const parseDirectAgentPayloadJson = (value: unknown): DirectAgentPayload => {
  if (typeof value !== "object" || value === null) throw invalidPayload();
  const record = value as Record<string, unknown>;
  if (typeof record.message !== "string") throw invalidPayload();
  const images = parseImages(record.images);
  return { message: record.message, ...(images === undefined ? {} : { images }) };
};

const parseImages = (value: unknown): DirectAgentPayload["images"] => {
  if (value === undefined) return undefined;
  if (!Array.isArray(value)) throw invalidPayload();
  return value.map((item) => {
    if (typeof item !== "object" || item === null) throw invalidPayload();
    const record = item as Record<string, unknown>;
    if (
      record.type !== "image" ||
      typeof record.data !== "string" ||
      typeof record.mimeType !== "string"
    ) {
      throw invalidPayload();
    }
    return { type: "image", data: record.data, mimeType: record.mimeType };
  });
};

const invalidPayload = (): Error =>
  new Error(
    'Direct agent requests must use JSON object body { "message": string, "images"?: image[] }.',
  );

const invalidPayloadResponse = (): Response =>
  jsonResponse({ error: { type: "invalid_request", message: invalidPayload().message } }, 400);

const streamUrl = (request: Request): string => {
  const url = new URL(request.url);
  url.search = "";
  return url.toString();
};

const jsonResponse = (body: unknown, status: number): Response =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });

export * as AgentRoutes from "./Routes.ts";
