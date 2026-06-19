import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as HttpBody from "effect/unstable/http/HttpBody";
import * as HttpClient from "effect/unstable/http/HttpClient";
import * as HttpApiClient from "effect/unstable/httpapi/HttpApiClient";
import { AgentThreadError } from "../../src/agent/Schema.ts";
import { Api } from "../../src/http/Api.ts";
import { Routes } from "../../src/http/Routes.ts";
import * as AgentThreadsMock from "../helpers/AgentThreadsMock.ts";
import * as AuthMock from "../helpers/AuthMock.ts";
import { makeDenoraUser } from "../helpers/fixtures.ts";
import * as ServerConfigMock from "../helpers/ServerConfigMock.ts";
import * as TestServer from "../helpers/TestServer.ts";

// The /me handler runs behind the Authorization middleware, which resolves the
// WorkOS session via Auth.requireSession. We swap the whole Auth port for a mock
// that treats any cookie containing "valid" as an authenticated session; the
// real WorkOS seal/verify path is out of scope here.
const validUser = makeDenoraUser();
const agentPath = `/agents/${validUser.id}`;

const appLayer = (agentThreadsLayer = AgentThreadsMock.layer()) =>
  TestServer.layer(
    Routes.layer.pipe(
      Layer.provide(
        AuthMock.layer((request) =>
          (request.headers.get("cookie") ?? "").includes("valid")
            ? Option.some(validUser)
            : Option.none(),
        ),
      ),
      Layer.provide(agentThreadsLayer),
      Layer.provide(ServerConfigMock.layer()),
    ),
  );

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

  describe("Agent group POST /agents/:agentId/threads/:threadId/messages", () => {
    it.effect("with no cookie is 401", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const res = yield* client.post(`${agentPath}/threads/thread_1/messages`, {
          body: HttpBody.jsonUnsafe({ message: "hello" }),
        });
        assert.strictEqual(res.status, 401);
      }).pipe(Effect.provide(appLayer())),
    );

    it.effect("with a valid session returns the assistant message", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const res = yield* client.post(`${agentPath}/threads/thread_1/messages`, {
          body: HttpBody.jsonUnsafe({ message: "hello" }),
          headers: {
            cookie: "denora_session=valid",
          },
        });

        assert.strictEqual(res.status, 200);
        assert.deepStrictEqual(yield* res.json, {
          threadId: "thread_1",
          agentId: validUser.id,
          role: "assistant",
          content: "Mock reply to: hello",
        });
      }).pipe(Effect.provide(appLayer())),
    );

    it.effect("rejects a signed-in user for a different agent id", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const res = yield* client.post("/agents/user_other/threads/thread_1/messages", {
          body: HttpBody.jsonUnsafe({ message: "hello" }),
          headers: {
            cookie: "denora_session=valid",
          },
        });

        assert.strictEqual(res.status, 401);
      }).pipe(Effect.provide(appLayer())),
    );
  });

  describe("Agent stream route POST /agents/:agentId/threads/:threadId/messages/stream", () => {
    it.effect("with no cookie is 401", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const res = yield* client.post(`${agentPath}/threads/thread_1/messages/stream`, {
          body: HttpBody.jsonUnsafe({ message: "hello" }),
        });
        assert.strictEqual(res.status, 401);
      }).pipe(Effect.provide(appLayer())),
    );

    it.effect("with a valid session streams assistant text chunks", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const res = yield* client.post(`${agentPath}/threads/thread_1/messages/stream`, {
          body: HttpBody.jsonUnsafe({ message: "hello" }),
          headers: {
            cookie: "denora_session=valid",
          },
        });

        assert.strictEqual(res.status, 200);
        assert.strictEqual(yield* res.text, "Mock stream reply to: hello");
      }).pipe(Effect.provide(appLayer())),
    );

    it.effect("rejects a signed-in user streaming to a different agent id", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const res = yield* client.post("/agents/user_other/threads/thread_1/messages/stream", {
          body: HttpBody.jsonUnsafe({ message: "hello" }),
          headers: {
            cookie: "denora_session=valid",
          },
        });

        assert.strictEqual(res.status, 403);
        assert.strictEqual(yield* res.text, "Agent does not belong to current user");
      }).pipe(Effect.provide(appLayer())),
    );

    it.effect("returns a 500 when the stream fails before the first chunk", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const res = yield* client.post(`${agentPath}/threads/thread_1/messages/stream`, {
          body: HttpBody.jsonUnsafe({ message: "hello" }),
          headers: {
            cookie: "denora_session=valid",
          },
        });

        assert.strictEqual(res.status, 500);
        assert.strictEqual(yield* res.text, "Model returned no text; model=@cf/test");
      }).pipe(
        Effect.provide(
          appLayer(
            AgentThreadsMock.layer({
              stream: () =>
                Stream.fail(
                  new AgentThreadError({
                    operation: "stream",
                    message: "Model returned no text",
                    model: "@cf/test",
                  }),
                ),
            }),
          ),
        ),
      ),
    );

    it.effect("preserves diagnostics from RPC-wrapped stream errors", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const res = yield* client.post(`${agentPath}/threads/thread_1/messages/stream`, {
          body: HttpBody.jsonUnsafe({ message: "hello" }),
          headers: {
            cookie: "denora_session=valid",
          },
        });

        assert.strictEqual(res.status, 500);
        assert.strictEqual(
          yield* res.text,
          "Assistant response completed without text; model=@cf/test; finishReason=stop; partTypes=finish; textLength=0",
        );
      }).pipe(
        Effect.provide(
          appLayer(
            AgentThreadsMock.layer({
              stream: () =>
                Stream.fail({
                  _tag: "RpcRemoteStreamError",
                  error: {
                    _tag: "AgentThreadError",
                    operation: "stream",
                    message: "Assistant response completed without text",
                    model: "@cf/test",
                    detail: "finishReason=stop; partTypes=finish; textLength=0",
                  },
                } as unknown as AgentThreadError),
            }),
          ),
        ),
      ),
    );

    it.effect("returns a 500 when the stream completes without chunks", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const res = yield* client.post(`${agentPath}/threads/thread_1/messages/stream`, {
          body: HttpBody.jsonUnsafe({ message: "hello" }),
          headers: {
            cookie: "denora_session=valid",
          },
        });

        assert.strictEqual(res.status, 500);
        assert.strictEqual(yield* res.text, "Assistant stream completed without text");
      }).pipe(
        Effect.provide(
          appLayer(
            AgentThreadsMock.layer({
              stream: () => Stream.empty,
            }),
          ),
        ),
      ),
    );
  });
});
