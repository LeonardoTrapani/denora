import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as HttpClient from "effect/unstable/http/HttpClient";
import * as HttpClientRequest from "effect/unstable/http/HttpClientRequest";
import * as HttpApiClient from "effect/unstable/httpapi/HttpApiClient";
import { PiRuntime } from "../../src/agent-loop/PiRuntime.ts";
import { AgentRuns } from "../../src/agent-run/AgentRuns.ts";
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
      AgentRuns.inMemoryLayer.pipe(
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

const waitForRunReplay = (client: HttpClient.HttpClient, runId: string) =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < 50; attempt += 1) {
      const response = yield* client.get(`/runs/${runId}`, {
        headers: { cookie: "denora_session=valid" },
      });
      const events = (yield* response.json) as ReadonlyArray<Record<string, unknown>>;
      if (response.headers["stream-closed"] === "true") return { response, events };
      yield* Effect.promise(() => new Promise((resolve) => setTimeout(resolve, 10)));
    }
    throw new Error(`Timed out waiting for run ${runId} to close.`);
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

  describe("Agent Run", () => {
    it.effect("creates a run and replays translated agent events", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const runId = `run_${crypto.randomUUID()}`;

        const created = yield* client.execute(
          HttpClientRequest.post("/agent-runs").pipe(
            HttpClientRequest.setHeader("cookie", "denora_session=valid"),
            HttpClientRequest.bodyJsonUnsafe({ runId, input: { prompt: "hello" } }),
          ),
        );
        assert.strictEqual(created.status, 200);
        const createdBody = (yield* created.json) as {
          readonly runId: string;
          readonly streamUrl: string;
          readonly streamPath: string;
          readonly offset: string;
        };
        assert.strictEqual(createdBody.runId, runId);
        assert.strictEqual(createdBody.streamPath, `runs/${runId}`);
        assert.strictEqual(createdBody.offset, "0000000000000000_0000000000000000");

        const { response: replay, events } = yield* waitForRunReplay(client, runId);
        assert.strictEqual(replay.status, 200);
        assert.strictEqual(replay.headers["stream-closed"], "true");
        assert.strictEqual(events[0]?.type, "run_start");
        assert.strictEqual(events[0]?.runId, runId);
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
            "run_end",
          ],
        );
        const textDelta = events.find((event) => event.type === "text_delta");
        assert.strictEqual(textDelta?.runId, runId);
        assert.strictEqual(textDelta?.text, "hello");
        const runEnd = events.find((event) => event.type === "run_end");
        assert.strictEqual(runEnd?.runId, runId);
        assert.strictEqual(runEnd?.outcome, "completed");
      }).pipe(Effect.provide(appLayer())),
    );

    it.effect("serves HEAD metadata for an existing run stream", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const runId = `run_${crypto.randomUUID()}`;

        const created = yield* client.execute(
          HttpClientRequest.post("/agent-runs").pipe(
            HttpClientRequest.setHeader("cookie", "denora_session=valid"),
            HttpClientRequest.bodyJsonUnsafe({ runId, input: { prompt: "hello" } }),
          ),
        );
        assert.strictEqual(created.status, 200);
        yield* waitForRunReplay(client, runId);

        const head = yield* client.execute(
          HttpClientRequest.head(`/runs/${runId}`).pipe(
            HttpClientRequest.setHeader("cookie", "denora_session=valid"),
          ),
        );

        assert.strictEqual(head.status, 200);
        assert.strictEqual(yield* head.text, "");
        assert.strictEqual(head.headers["stream-up-to-date"], "true");
        assert.strictEqual(head.headers["stream-closed"], "true");
        assert.isString(head.headers.etag);
        assert.strictEqual(head.headers["x-content-type-options"], "nosniff");
        assert.strictEqual(head.headers["cross-origin-resource-policy"], "cross-origin");
      }).pipe(Effect.provide(appLayer())),
    );

    it.effect("keeps image bytes in model input but redacts public run stream events", () => {
      const fake = fakeGateway();
      return Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const runId = `run_${crypto.randomUUID()}`;

        const created = yield* client.execute(
          HttpClientRequest.post("/agent-runs").pipe(
            HttpClientRequest.setHeader("cookie", "denora_session=valid"),
            HttpClientRequest.bodyJsonUnsafe({
              runId,
              input: {
                messages: [
                  {
                    role: "user",
                    content: [
                      { type: "text", text: "describe this image" },
                      { type: "image", data: IMAGE_BYTES, mimeType: "image/png" },
                    ],
                    timestamp: Date.now(),
                  },
                ],
              },
            }),
          ),
        );
        assert.strictEqual(created.status, 200);

        const { events } = yield* waitForRunReplay(client, runId);
        const replayJson = JSON.stringify(events);
        const runStart = events.find((event) => event.type === "run_start");

        assert.include(JSON.stringify(fake.calls[0]?.payload), IMAGE_BYTES);
        assert.notInclude(replayJson, IMAGE_BYTES);
        assert.notProperty(runStart ?? {}, "input");
      }).pipe(Effect.provide(appLayer(fake)));
    });

    it.effect("serves an empty 404 HEAD response for a missing run stream", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const runId = `run_${crypto.randomUUID()}`;

        const head = yield* client.execute(
          HttpClientRequest.head(`/runs/${runId}`).pipe(
            HttpClientRequest.setHeader("cookie", "denora_session=valid"),
          ),
        );

        assert.strictEqual(head.status, 404);
        assert.strictEqual(yield* head.text, "");
      }).pipe(Effect.provide(appLayer())),
    );

    it.effect("returns structured unauthorized errors for run streams", () =>
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const res = yield* client.get("/runs/run_missing");

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
        const res = yield* client.get("/runs/run_invalid?offset=banana", {
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
