import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as HttpClient from "effect/unstable/http/HttpClient";
import * as HttpClientRequest from "effect/unstable/http/HttpClientRequest";
import * as HttpApiClient from "effect/unstable/httpapi/HttpApiClient";
import { AgentRuns } from "../../src/agent-run/AgentRuns.ts";
import { Api } from "../../src/http/Api.ts";
import { Routes } from "../../src/http/Routes.ts";
import * as AuthMock from "../helpers/AuthMock.ts";
import { makeDenoraUser } from "../helpers/fixtures.ts";
import * as ServerConfigMock from "../helpers/ServerConfigMock.ts";
import * as TestServer from "../helpers/TestServer.ts";

// The /me handler runs behind the Authorization middleware, which resolves the
// WorkOS session via Auth.requireSession. We swap the whole Auth port for a mock
// that treats any cookie containing "valid" as an authenticated session; the
// real WorkOS seal/verify path is out of scope here.
const validUser = makeDenoraUser();

const appLayer = () =>
  TestServer.layer(Routes.layer).pipe(
    Layer.provide([
      AuthMock.layer((request) =>
        (request.headers.get("cookie") ?? "").includes("valid")
          ? Option.some(validUser)
          : Option.none(),
      ),
      AgentRuns.inMemoryLayer,
      ServerConfigMock.layer(),
    ]),
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

  describe("Agent Run", () => {
    it.effect("creates a run and replays its initial run_start event", () =>
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

        const replay = yield* client.get(`/runs/${runId}`, {
          headers: { cookie: "denora_session=valid" },
        });
        assert.strictEqual(replay.status, 200);
        assert.strictEqual(replay.headers["stream-next-offset"], createdBody.offset);
        const events = (yield* replay.json) as ReadonlyArray<Record<string, unknown>>;
        assert.strictEqual(events.length, 1);
        assert.strictEqual(events[0]?.type, "run_start");
        assert.strictEqual(events[0]?.runId, runId);
      }).pipe(Effect.provide(appLayer())),
    );
  });
});
