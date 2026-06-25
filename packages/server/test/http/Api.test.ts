import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as HttpClient from "effect/unstable/http/HttpClient";
import * as HttpClientRequest from "effect/unstable/http/HttpClientRequest";
import * as HttpApiClient from "effect/unstable/httpapi/HttpApiClient";
import { PiRuntime } from "../../src/agent-loop/PiRuntime.ts";
import { Conversations } from "../../src/conversation/Conversations.ts";
import { Api } from "../../src/http/Api.ts";
import { Routes } from "../../src/http/Routes.ts";
import * as AuthMock from "../helpers/AuthMock.ts";
import { FakeAiGateway } from "../helpers/FakeAiGateway.ts";
import { makeDenoraUser } from "../helpers/fixtures.ts";
import * as ServerConfigMock from "../helpers/ServerConfigMock.ts";
import * as TestServer from "../helpers/TestServer.ts";

// The /me handler runs behind the Authorization middleware, which resolves the
// WorkOS session via Auth.requireSession. We swap the whole Auth port for a mock
// that treats any cookie containing "valid" as an authenticated session; the
// real WorkOS seal/verify path is out of scope here.
const validUser = makeDenoraUser();
const IMAGE_BYTES = "aGVsbG8taW1hZ2UtYnl0ZXM=";

const appLayer = (fake = fakeGateway()) =>
  TestServer.layer(Routes.layer).pipe(
    Layer.provide([
      AuthMock.layer((request) =>
        (request.headers.get("cookie") ?? "").includes("valid")
          ? Option.some(validUser)
          : Option.none(),
      ),
      Conversations.inMemoryLayer.pipe(
        Layer.provide(PiRuntime.layer.pipe(Layer.provide(FakeAiGateway.layer(fake)))),
      ),
      ServerConfigMock.layer(),
    ]),
  );

const fakeGateway = () =>
  FakeAiGateway.make(
    FakeAiGateway.sse(
      FakeAiGateway.json({ choices: [{ delta: { content: "hello" } }] }),
      FakeAiGateway.json({ choices: [{ finish_reason: "stop" }] }),
      FakeAiGateway.done(),
    ),
  );

const waitForConversationEvent = (
  client: HttpClient.HttpClient,
  conversationId: string,
  agentName = "default",
  path = `/agents/${agentName}/${conversationId}`,
) =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < 50; attempt += 1) {
      const response = yield* client.get(path, {
        headers: { cookie: "denora_session=valid" },
      });
      const events = (yield* response.json) as ReadonlyArray<Record<string, unknown>>;
      if (events.some((event) => event.type === "submission_settled")) return { response, events };
      yield* Effect.promise(() => new Promise((resolve) => setTimeout(resolve, 10)));
    }
    throw new Error(`Timed out waiting for conversation ${conversationId} events.`);
  });

describe("Api http surface", () => {
  describe("System group (topLevel) GET /health", () => {
    it.effect("returns 200 with json { status: 'ok' }", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const res = yield* client.get("/health");
        assert.strictEqual(res.status, 200);
        assert.deepStrictEqual(yield* res.json, { status: "ok" });
      }).pipe(Effect.provide(appLayer())),
    );

    // System is a topLevel group, so the generated client exposes the endpoint
    // directly on the root (client.health()) rather than client.System.health().
    it.effect("is reachable via the generated typed client and decodes to a Health", () =>
      Effect.gen(function* () {
        const client = yield* HttpApiClient.make(Api.DenoraApi);
        const health = yield* client.health();
        assert.strictEqual(health.status, "ok");
      }).pipe(Effect.provide(appLayer())),
    );
  });

  describe("Account group GET /me", () => {
    it.effect("with no cookie is 401", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const res = yield* client.get("/me");
        assert.strictEqual(res.status, 401);
      }).pipe(Effect.provide(appLayer())),
    );

    it.effect("with an invalid session cookie is 401", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const res = yield* client.get("/me", {
          headers: { cookie: "denora_session=bogus" },
        });
        assert.strictEqual(res.status, 401);
      }).pipe(Effect.provide(appLayer())),
    );

    it.effect("with a valid session returns 200 and the DenoraUser body", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const res = yield* client.get("/me", {
          headers: { cookie: "denora_session=valid" },
        });
        assert.strictEqual(res.status, 200);
        assert.deepStrictEqual(yield* res.json, {
          id: validUser.id,
          email: validUser.email,
          emailVerified: validUser.emailVerified,
          name: validUser.name,
          image: validUser.image,
          createdAt: validUser.createdAt,
          updatedAt: validUser.updatedAt,
        });
      }).pipe(Effect.provide(appLayer())),
    );
  });

  describe("Conversation", () => {
    it.effect("creates a conversation and replays attached-agent events", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const conversationId = `conversation_${crypto.randomUUID()}`;

        const createdConversation = yield* client.execute(
          HttpClientRequest.post("/conversations").pipe(
            HttpClientRequest.setHeader("cookie", "denora_session=valid"),
            HttpClientRequest.bodyJsonUnsafe({ conversationId, title: "Test" }),
          ),
        );
        assert.strictEqual(createdConversation.status, 200);

        const created = yield* client.execute(
          HttpClientRequest.post(`/conversations/${conversationId}/messages`).pipe(
            HttpClientRequest.setHeader("cookie", "denora_session=valid"),
            HttpClientRequest.bodyJsonUnsafe({ message: "hello" }),
          ),
        );
        assert.strictEqual(created.status, 202);
        const createdBody = (yield* created.json) as {
          readonly conversationId: string;
          readonly messageId: string;
          readonly submissionId: string;
          readonly runId: string;
          readonly streamUrl: string;
          readonly streamPath: string;
          readonly offset: string;
        };
        assert.strictEqual(createdBody.conversationId, conversationId);
        assert.strictEqual(createdBody.streamPath, `conversations/${conversationId}`);
        assert.strictEqual(
          new URL(createdBody.streamUrl).pathname,
          `/conversations/${conversationId}/events`,
        );
        assert.strictEqual(createdBody.offset, "-1");
        assert.strictEqual(created.headers.location, createdBody.streamUrl);
        assert.strictEqual(created.headers["stream-next-offset"], createdBody.offset);

        const { response: replay, events } = yield* waitForConversationEvent(
          client,
          conversationId,
          "default",
          `/conversations/${conversationId}/events`,
        );
        assert.strictEqual(replay.status, 200);
        assert.strictEqual(events[0]?.type, "message_start");
        assert.strictEqual(events[0]?.instanceId, conversationId);
        assert.strictEqual(events[0]?.agentName, "default");
        assert.strictEqual(events[0]?.submissionId, createdBody.submissionId);
        assert.notProperty(events[0] ?? {}, "runId");
        assert.strictEqual(events[0]?.v, 3);
        assert.includeMembers(
          events.map((event) => event.type),
          [
            "agent_start",
            "turn_start",
            "message_start",
            "text_delta",
            "turn",
            "message_end",
            "turn_messages",
            "agent_end",
            "submission_settled",
            "idle",
          ],
        );
        assert.notInclude(
          events.map((event) => event.type),
          "run_start",
        );
        assert.notInclude(
          events.map((event) => event.type),
          "run_end",
        );
        const textDelta = events.find((event) => event.type === "text_delta");
        assert.strictEqual(textDelta?.instanceId, conversationId);
        assert.strictEqual(textDelta?.agentName, "default");
        assert.strictEqual(textDelta?.submissionId, createdBody.submissionId);
        assert.notProperty(textDelta ?? {}, "runId");
        assert.strictEqual(textDelta?.text, "hello");
        const settled = events.find((event) => event.type === "submission_settled");
        assert.strictEqual(settled?.submissionId, createdBody.submissionId);
        assert.strictEqual(settled?.outcome, "completed");

        const messages = yield* client.get(`/conversations/${conversationId}/messages`, {
          headers: { cookie: "denora_session=valid" },
        });
        assert.strictEqual(messages.status, 200);
        const messageBody = (yield* messages.json) as ReadonlyArray<Record<string, unknown>>;
        assert.strictEqual(messageBody[0]?.id, createdBody.messageId);
        assert.strictEqual(messageBody[0]?.role, "user");
      }).pipe(Effect.provide(appLayer())),
    );

    it.effect("submits and streams through the Flue-compatible attached-agent route", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const conversationId = `conversation_${crypto.randomUUID()}`;
        const created = yield* client.execute(
          HttpClientRequest.post(`/agents/denora/${conversationId}`).pipe(
            HttpClientRequest.setHeader("cookie", "denora_session=valid"),
            HttpClientRequest.bodyJsonUnsafe({ message: "hello" }),
          ),
        );

        assert.strictEqual(created.status, 202);
        const createdBody = (yield* created.json) as {
          readonly streamUrl: string;
          readonly offset: string;
          readonly submissionId: string;
          readonly runId?: string;
        };
        assert.strictEqual(
          new URL(createdBody.streamUrl).pathname,
          `/agents/denora/${conversationId}`,
        );
        assert.strictEqual(createdBody.offset, "-1");
        assert.match(createdBody.submissionId, /^submission_/);
        assert.notProperty(createdBody, "runId");

        const { events } = yield* waitForConversationEvent(client, conversationId, "denora");
        const userStart = events.find((event) => event.type === "message_start");
        assert.strictEqual(userStart?.instanceId, conversationId);
        assert.strictEqual(userStart?.agentName, "denora");
        assert.strictEqual(userStart?.submissionId, createdBody.submissionId);
        assert.notProperty(userStart ?? {}, "runId");
        assert.includeMembers(
          events.map((event) => event.type),
          ["message_start", "text_delta", "submission_settled", "idle"],
        );
      }).pipe(Effect.provide(appLayer())),
    );

    it.effect("archives a conversation and rejects later submissions", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const conversationId = `conversation_${crypto.randomUUID()}`;

        const created = yield* client.execute(
          HttpClientRequest.post("/conversations").pipe(
            HttpClientRequest.setHeader("cookie", "denora_session=valid"),
            HttpClientRequest.bodyJsonUnsafe({ conversationId }),
          ),
        );
        assert.strictEqual(created.status, 200);

        const archived = yield* client.execute(
          HttpClientRequest.post(`/conversations/${conversationId}/archive`).pipe(
            HttpClientRequest.setHeader("cookie", "denora_session=valid"),
          ),
        );
        const archivedAgain = yield* client.execute(
          HttpClientRequest.post(`/conversations/${conversationId}/archive`).pipe(
            HttpClientRequest.setHeader("cookie", "denora_session=valid"),
          ),
        );
        const rejected = yield* client.execute(
          HttpClientRequest.post(`/conversations/${conversationId}/messages`).pipe(
            HttpClientRequest.setHeader("cookie", "denora_session=valid"),
            HttpClientRequest.bodyJsonUnsafe({ message: "must fail" }),
          ),
        );

        assert.strictEqual(archived.status, 200);
        assert.strictEqual(
          ((yield* archived.json) as { readonly status?: string }).status,
          "archived",
        );
        assert.strictEqual(archivedAgain.status, 200);
        assert.strictEqual(
          ((yield* archivedAgain.json) as { readonly status?: string }).status,
          "archived",
        );
        assert.strictEqual(rejected.status, 500);
      }).pipe(Effect.provide(appLayer())),
    );

    it.effect("deletes a conversation and rejects later submissions", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const conversationId = `conversation_${crypto.randomUUID()}`;

        const created = yield* client.execute(
          HttpClientRequest.post("/conversations").pipe(
            HttpClientRequest.setHeader("cookie", "denora_session=valid"),
            HttpClientRequest.bodyJsonUnsafe({ conversationId }),
          ),
        );
        assert.strictEqual(created.status, 200);

        const deleted = yield* client.execute(
          HttpClientRequest.post(`/conversations/${conversationId}/delete`).pipe(
            HttpClientRequest.setHeader("cookie", "denora_session=valid"),
          ),
        );
        const deletedAgain = yield* client.execute(
          HttpClientRequest.post(`/conversations/${conversationId}/delete`).pipe(
            HttpClientRequest.setHeader("cookie", "denora_session=valid"),
          ),
        );
        const rejected = yield* client.execute(
          HttpClientRequest.post(`/conversations/${conversationId}/messages`).pipe(
            HttpClientRequest.setHeader("cookie", "denora_session=valid"),
            HttpClientRequest.bodyJsonUnsafe({ message: "must fail" }),
          ),
        );

        assert.strictEqual(deleted.status, 200);
        assert.strictEqual(
          ((yield* deleted.json) as { readonly status?: string }).status,
          "deleted",
        );
        assert.strictEqual(deletedAgain.status, 200);
        assert.strictEqual(
          ((yield* deletedAgain.json) as { readonly status?: string }).status,
          "deleted",
        );
        assert.strictEqual(rejected.status, 500);
      }).pipe(Effect.provide(appLayer())),
    );

    it.effect("omits stream location headers for attached-agent wait=result responses", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const conversationId = `conversation_${crypto.randomUUID()}`;
        const response = yield* client.execute(
          HttpClientRequest.post(`/agents/denora/${conversationId}?wait=result`).pipe(
            HttpClientRequest.setHeader("cookie", "denora_session=valid"),
            HttpClientRequest.bodyJsonUnsafe({ message: "hello" }),
          ),
        );

        assert.strictEqual(response.status, 200);
        assert.isUndefined(response.headers.location);
        assert.isUndefined(response.headers["stream-next-offset"]);
        const body = (yield* response.json) as Record<string, unknown>;
        assert.property(body, "result");
        assert.match(String(body.submissionId), /^submission_/);
      }).pipe(Effect.provide(appLayer())),
    );

    it.effect("serves HEAD metadata for an existing conversation stream", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const conversationId = `conversation_${crypto.randomUUID()}`;

        yield* client.execute(
          HttpClientRequest.post("/conversations").pipe(
            HttpClientRequest.setHeader("cookie", "denora_session=valid"),
            HttpClientRequest.bodyJsonUnsafe({ conversationId }),
          ),
        );

        const created = yield* client.execute(
          HttpClientRequest.post(`/conversations/${conversationId}/messages`).pipe(
            HttpClientRequest.setHeader("cookie", "denora_session=valid"),
            HttpClientRequest.bodyJsonUnsafe({ message: "hello" }),
          ),
        );
        assert.strictEqual(created.status, 202);
        yield* waitForConversationEvent(client, conversationId);

        const head = yield* client.execute(
          HttpClientRequest.head(`/agents/default/${conversationId}`).pipe(
            HttpClientRequest.setHeader("cookie", "denora_session=valid"),
          ),
        );

        assert.strictEqual(head.status, 200);
        assert.strictEqual(yield* head.text, "");
        assert.strictEqual(head.headers["stream-up-to-date"], "true");
        assert.isUndefined(head.headers["stream-closed"]);
        assert.isString(head.headers.etag);
        assert.strictEqual(head.headers["x-content-type-options"], "nosniff");
        assert.strictEqual(head.headers["cross-origin-resource-policy"], "cross-origin");
      }).pipe(Effect.provide(appLayer())),
    );

    it.effect(
      "keeps image bytes in model input but redacts public conversation stream events",
      () => {
        const fake = fakeGateway();
        return Effect.gen(function* () {
          const client = yield* HttpClient.HttpClient;
          const conversationId = `conversation_${crypto.randomUUID()}`;

          yield* client.execute(
            HttpClientRequest.post("/conversations").pipe(
              HttpClientRequest.setHeader("cookie", "denora_session=valid"),
              HttpClientRequest.bodyJsonUnsafe({ conversationId }),
            ),
          );

          const created = yield* client.execute(
            HttpClientRequest.post(`/conversations/${conversationId}/messages`).pipe(
              HttpClientRequest.setHeader("cookie", "denora_session=valid"),
              HttpClientRequest.bodyJsonUnsafe({
                content: {
                  text: "describe this image",
                  image: { type: "image", data: IMAGE_BYTES, mimeType: "image/png" },
                },
              }),
            ),
          );
          assert.strictEqual(created.status, 202);

          const { events } = yield* waitForConversationEvent(client, conversationId);
          const replayJson = JSON.stringify(events);
          const userMessage = events.find((event) => event.type === "message_start");

          const modelRequest = fake.calls[0]?.payload ?? fake.gatewayCalls[0]?.request;
          assert.include(JSON.stringify(modelRequest), IMAGE_BYTES);
          assert.notInclude(replayJson, IMAGE_BYTES);
          assert.property(userMessage ?? {}, "message");
        }).pipe(Effect.provide(appLayer(fake)));
      },
    );

    it.effect("serves an empty 404 HEAD response for a missing conversation stream", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const conversationId = `conversation_${crypto.randomUUID()}`;

        yield* client.execute(
          HttpClientRequest.post("/conversations").pipe(
            HttpClientRequest.setHeader("cookie", "denora_session=valid"),
            HttpClientRequest.bodyJsonUnsafe({ conversationId }),
          ),
        );

        const head = yield* client.execute(
          HttpClientRequest.head(`/agents/default/${conversationId}`).pipe(
            HttpClientRequest.setHeader("cookie", "denora_session=valid"),
          ),
        );

        assert.strictEqual(head.status, 404);
        assert.strictEqual(yield* head.text, "");
      }).pipe(Effect.provide(appLayer())),
    );

    it.effect("returns structured unauthorized errors for conversation streams", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const res = yield* client.get("/agents/default/conversation_missing");

        assert.strictEqual(res.status, 401);
        assert.deepStrictEqual(yield* res.json, {
          error: {
            type: "unauthorized",
            message: "Authentication is required.",
          },
        });
      }).pipe(Effect.provide(appLayer())),
    );

    it.effect("returns structured stream validation errors", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const conversationId = `conversation_${crypto.randomUUID()}`;

        yield* client.execute(
          HttpClientRequest.post("/conversations").pipe(
            HttpClientRequest.setHeader("cookie", "denora_session=valid"),
            HttpClientRequest.bodyJsonUnsafe({ conversationId }),
          ),
        );

        const res = yield* client.get(`/agents/default/${conversationId}?offset=banana`, {
          headers: { cookie: "denora_session=valid" },
        });

        assert.strictEqual(res.status, 400);
        assert.deepStrictEqual(yield* res.json, {
          error: {
            type: "invalid_request",
            code: "invalid_offset_format",
            message: "Invalid stream offset format.",
            details: { offset: "banana" },
          },
        });
      }).pipe(Effect.provide(appLayer())),
    );
  });
});
