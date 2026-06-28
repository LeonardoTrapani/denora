import * as NodeFs from "node:fs";
import * as NodePath from "node:path";
import { fileURLToPath } from "node:url";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import * as HttpServer from "effect/unstable/http/HttpServer";
import { PiRuntime } from "../../src/agent-loop/PiRuntime.ts";
import { AgentRuns } from "../../src/agent-run/AgentRuns.ts";
import { DenoraUser, Unauthorized } from "../../src/auth/User.ts";
import { Client } from "../../src/Client.ts";
import { ConversationDomain } from "../../src/conversation/ConversationDomain.ts";
import { Conversations } from "../../src/conversation/Conversations.ts";
import {
  Conversation,
  ConversationMessage,
  CreateConversationPayload,
  SubmitConversationMessagePayload,
  SubmitConversationMessageResponse,
} from "../../src/http/conversation/Api.ts";
import { Routes } from "../../src/http/Routes.ts";
import { Health } from "../../src/http/system/Schema.ts";
import * as AuthMock from "../helpers/AuthMock.ts";
import { FakeAiGateway } from "../helpers/FakeAiGateway.ts";
import { makeDenoraUser } from "../helpers/fixtures.ts";
import * as ServerConfigMock from "../helpers/ServerConfigMock.ts";
import * as TestServer from "../helpers/TestServer.ts";

// =============================================================================
// SCHEMA
// =============================================================================

describe("schema: Health", () => {
  it("decodes { status: 'ok' }", () => {
    const health = Schema.decodeSync(Health)({ status: "ok" });
    assert.strictEqual(health.status, "ok");
    assert.deepStrictEqual(health, { status: "ok" });
  });

  it("rejects a non-'ok' status", () => {
    const decode = Schema.decodeUnknownOption(Health);
    assert.isTrue(Option.isNone(decode({ status: "other" })));
    assert.isTrue(Option.isNone(decode({ status: "OK" })));
  });

  it("rejects a missing status field", () => {
    assert.isTrue(Option.isNone(Schema.decodeUnknownOption(Health)({})));
  });

  it("throws on a bad status via decodeSync", () => {
    assert.throws(() => Schema.decodeSync(Health)({ status: "nope" } as never));
  });
});

describe("schema: DenoraUser", () => {
  const fullEncoded = {
    id: "00000000-0000-0000-0000-000000000001",
    email: "ada@example.com",
    emailVerified: true,
    name: "Ada Lovelace",
    image: "https://example.com/ada.png",
    createdAt: "2026-01-01T00:00:00.000Z",
    updatedAt: "2026-01-02T00:00:00.000Z",
  };

  it("round-trips a fully-populated user (decode then encode)", () => {
    const user = Schema.decodeSync(DenoraUser)(fullEncoded);
    assert.deepStrictEqual(user, fullEncoded);
    assert.strictEqual(user.email, "ada@example.com");
    assert.strictEqual(user.emailVerified, true);
    assert.deepStrictEqual(Schema.encodeSync(DenoraUser)(user), fullEncoded);
  });

  it("round-trips with null nullable fields", () => {
    const encoded = { ...fullEncoded, name: null, image: null };
    const user = Schema.decodeSync(DenoraUser)(encoded);
    assert.strictEqual(user.name, null);
    assert.strictEqual(user.image, null);
    assert.deepStrictEqual(Schema.encodeSync(DenoraUser)(user), encoded);
  });

  it("rejects a missing required field", () => {
    const { email, ...withoutEmail } = fullEncoded;
    void email;
    assert.isTrue(Option.isNone(Schema.decodeUnknownOption(DenoraUser)(withoutEmail)));
  });

  it("rejects a wrong-typed field (emailVerified as string)", () => {
    const bad = { ...fullEncoded, emailVerified: "yes" };
    assert.isTrue(Option.isNone(Schema.decodeUnknownOption(DenoraUser)(bad)));
  });

  it.effect.prop("encode then decode is identity", [DenoraUser], ([user]) =>
    Effect.sync(() => {
      const encoded = Schema.encodeSync(DenoraUser)(user);
      const decoded = Schema.decodeSync(DenoraUser)(encoded);
      assert.deepStrictEqual(Schema.encodeSync(DenoraUser)(decoded), encoded);
    }),
  );
});

describe("schema: conversation payloads", () => {
  it("accept plain objects for API request payloads", () => {
    assert.deepStrictEqual(
      Schema.decodeUnknownSync(CreateConversationPayload)({
        title: "New conversation",
      }),
      {
        title: "New conversation",
      },
    );
    assert.deepStrictEqual(
      Schema.decodeUnknownSync(SubmitConversationMessagePayload)({
        message: "hello",
      }),
      {
        message: "hello",
      },
    );
  });

  it("accepts non-empty custom conversation ids in create payloads", () => {
    const decoded = Schema.decodeUnknownSync(CreateConversationPayload)({
      conversationId: "550e8400-e29b-41d4-a716-446655440000",
    });
    assert.strictEqual(decoded.conversationId, "550e8400-e29b-41d4-a716-446655440000");
    assert.isTrue(
      Option.isNone(
        Schema.decodeUnknownOption(CreateConversationPayload)({
          conversationId: "",
        }),
      ),
    );
  });

  it("accepts legacy non-empty ids in conversation list responses and rejects empty ids", () => {
    const listSchema = Schema.Array(Conversation);
    const valid = {
      id: "550e8400-e29b-41d4-a716-446655440000",
      ownerUserId: "legacy-user-id",
      agentId: null,
      status: "active",
      title: "Legacy conversation",
      metadata: {},
      createdAt: "2026-01-01T00:00:00.000Z",
      updatedAt: "2026-01-01T00:00:00.000Z",
      archivedAt: null,
    };

    const decoded = Schema.decodeUnknownSync(listSchema)([valid]);
    assert.strictEqual(decoded[0]?.id, valid.id);
    assert.isTrue(Option.isNone(Schema.decodeUnknownOption(listSchema)([{ ...valid, id: "" }])));
  });

  it("accepts legacy non-empty ids in message list responses and rejects empty ids", () => {
    const listSchema = Schema.Array(ConversationMessage);
    const valid = {
      id: "legacy-message-id",
      conversationId: "550e8400-e29b-41d4-a716-446655440000",
      runId: "legacy-run-id",
      role: "user",
      content: "hello",
      metadata: {},
      createdAt: "2026-01-01T00:00:00.000Z",
    };

    const decoded = Schema.decodeUnknownSync(listSchema)([valid]);
    assert.strictEqual(decoded[0]?.id, valid.id);
    assert.strictEqual(decoded[0]?.conversationId, valid.conversationId);
    assert.strictEqual(decoded[0]?.runId, valid.runId);
    assert.isTrue(Option.isNone(Schema.decodeUnknownOption(listSchema)([{ ...valid, id: "" }])));
    assert.isTrue(
      Option.isNone(Schema.decodeUnknownOption(listSchema)([{ ...valid, conversationId: "" }])),
    );
    assert.isTrue(Option.isNone(Schema.decodeUnknownOption(listSchema)([{ ...valid, runId: "" }])));
  });

  it("rejects empty branded ids in submit message responses", () => {
    const valid = {
      conversationId: "conversation_123",
      messageId: "message_123",
      submissionId: "submission_123",
      runId: "run_123",
      streamUrl: "http://api.test/conversations/conversation_123/events",
      streamPath: "/runs/run_123/events",
      offset: "0",
    };

    assert.isTrue(
      Option.isNone(
        Schema.decodeUnknownOption(SubmitConversationMessageResponse)({
          ...valid,
          conversationId: "",
        }),
      ),
    );
    assert.isTrue(
      Option.isNone(
        Schema.decodeUnknownOption(SubmitConversationMessageResponse)({
          ...valid,
          messageId: "",
        }),
      ),
    );
    assert.isTrue(
      Option.isNone(
        Schema.decodeUnknownOption(SubmitConversationMessageResponse)({
          ...valid,
          submissionId: "",
        }),
      ),
    );
    assert.isTrue(
      Option.isNone(
        Schema.decodeUnknownOption(SubmitConversationMessageResponse)({
          ...valid,
          runId: "",
        }),
      ),
    );
  });
});

describe("schema: Unauthorized", () => {
  it("constructs with _tag and message", () => {
    const err = new Unauthorized({ message: "Missing or invalid session" });
    assert.strictEqual(err._tag, "Unauthorized");
    assert.strictEqual(err.message, "Missing or invalid session");
    assert.instanceOf(err, Unauthorized);
  });

  it("round-trips through its own schema", () => {
    const err = new Unauthorized({ message: "nope" });
    const encoded = Schema.encodeSync(Unauthorized)(err);
    assert.strictEqual(encoded._tag, "Unauthorized");
    assert.strictEqual(encoded.message, "nope");
    const decoded = Schema.decodeSync(Unauthorized)(encoded);
    assert.strictEqual(decoded._tag, "Unauthorized");
    assert.strictEqual(decoded.message, "nope");
  });
});

// =============================================================================
// CLIENT-SAFE IMPORT GRAPH
// =============================================================================

const serverSrcDir = fileURLToPath(new URL("../../src/", import.meta.url));

const clientSafeInternalFiles = [
  /^Client(?:Api)?\.ts$/,
  /^http\/Api\.ts$/,
  /^http\/(?:account|agent-run|ai|conversation|system)\/(?:Api|Errors|Schema)\.ts$/,
  /^auth\/(?:AuthorizationApi|User)\.ts$/,
  /^conversation\/ConversationDomain\.ts$/,
];

const serverOnlyExternalPrefixes = [
  "alchemy",
  "@aws-sdk/",
  "@distilled.cloud/",
  "@earendil-works/",
  "@effect/sql-pg",
  "@workos-inc/",
  "drizzle-orm",
  "pg",
];

const serverOnlyInternalFiles = [
  /^agent-loop\//,
  /^agent-run\/(?!Errors\.ts$)/,
  /^conversation\/(?!ConversationDomain\.ts$)/,
  /^http\/Routes\.ts$/,
  /^http\/.*\/Handlers\.ts$/,
  /^observability\//,
  /^persistence\//,
  /^server\//,
];

const runtimeImportSpecs = (source: string): ReadonlyArray<string> => {
  const specs: string[] = [];
  const patterns = [
    /^\s*import\s+(?!type\b)[\s\S]*?\sfrom\s*["']([^"']+)["']/gm,
    /^\s*import\s*["']([^"']+)["']/gm,
    /^\s*export\s+(?!type\b)(?:\*|\{[\s\S]*?\})\s*(?:as\s+\w+\s*)?from\s*["']([^"']+)["']/gm,
  ];

  for (const pattern of patterns) {
    for (const match of source.matchAll(pattern)) {
      const spec = match[1];
      if (spec !== undefined) specs.push(spec);
    }
  }

  return specs;
};

const resolveInternalImport = (fromFile: string, spec: string): string | undefined => {
  if (!spec.startsWith(".")) return undefined;
  const fromDir = NodePath.dirname(NodePath.join(serverSrcDir, fromFile));
  const resolved = NodePath.resolve(fromDir, spec);
  const candidates = [resolved, `${resolved}.ts`, NodePath.join(resolved, "index.ts")];
  const found = candidates.find((candidate) => NodeFs.existsSync(candidate));
  return found === undefined ? undefined : NodePath.relative(serverSrcDir, found);
};

const collectRuntimeGraph = (entry: string) => {
  const seen = new Set<string>();
  const externals = new Set<string>();
  const stack = [entry];

  while (stack.length > 0) {
    const file = stack.pop();
    if (file === undefined || seen.has(file)) continue;
    seen.add(file);

    const source = NodeFs.readFileSync(NodePath.join(serverSrcDir, file), "utf8");
    for (const spec of runtimeImportSpecs(source)) {
      const internal = resolveInternalImport(file, spec);
      if (internal === undefined) {
        if (!spec.startsWith(".")) externals.add(spec);
      } else if (!seen.has(internal)) {
        stack.push(internal);
      }
    }
  }

  return { files: [...seen].sort(), externals: [...externals].sort() };
};

describe("client import graph", () => {
  it("keeps @denora/server/client-api browser-safe", () => {
    const graph = collectRuntimeGraph("ClientApi.ts");
    const nonClientSafeFiles = graph.files.filter(
      (file) => !clientSafeInternalFiles.some((pattern) => pattern.test(file)),
    );
    const serverOnlyFiles = graph.files.filter((file) =>
      serverOnlyInternalFiles.some((pattern) => pattern.test(file)),
    );
    const serverOnlyExternals = graph.externals.filter((spec) =>
      serverOnlyExternalPrefixes.some((prefix) => spec === prefix || spec.startsWith(prefix)),
    );

    assert.deepStrictEqual(nonClientSafeFiles, []);
    assert.deepStrictEqual(serverOnlyFiles, []);
    assert.deepStrictEqual(serverOnlyExternals, []);
  });
});

// =============================================================================
// CLIENT
// =============================================================================

describe("client: makeDenoraUrlBuilder", () => {
  // Both `System` (health) and `Account` (me) groups are `topLevel: true`, so
  // the url builder exposes the endpoint names directly at the top level.
  it("builds absolute urls for the top-level endpoints", () => {
    const build = Client.makeDenoraUrlBuilder("http://api.test");
    assert.strictEqual(build.health(), "http://api.test/health");
    assert.strictEqual(build.me(), "http://api.test/me");
  });

  it("preserves a base path and normalizes trailing slashes", () => {
    const build = Client.makeDenoraUrlBuilder("http://api.test/");
    assert.strictEqual(build.health(), "http://api.test/health");
  });

  it("accepts a URL instance as the base", () => {
    const build = Client.makeDenoraUrlBuilder(new URL("https://app.denora.me"));
    assert.strictEqual(build.health(), "https://app.denora.me/health");
    assert.strictEqual(build.me(), "https://app.denora.me/me");
  });
});

const piLayer = PiRuntime.layer.pipe(Layer.provide(FakeAiGateway.layer(FakeAiGateway.make())));

const appLayer = TestServer.layer(Routes.layer).pipe(
  Layer.provide([
    AuthMock.layer(() => Option.some(makeDenoraUser())),
    Conversations.inMemoryLayer.pipe(Layer.provide(piLayer)),
    AgentRuns.inMemoryLayer.pipe(Layer.provide(piLayer)),
    ServerConfigMock.layer(),
  ]),
);
const serverLayer = appLayer;

describe("client: makeDenoraClient (real round-trip)", () => {
  it.effect("calls the health endpoint against the served app", () =>
    Effect.gen(function* () {
      const server = yield* HttpServer.HttpServer;
      const address = server.address;
      assert.strictEqual(address._tag, "TcpAddress");
      const port = address._tag === "TcpAddress" ? address.port : 0;
      assert.isTrue(port > 0);

      const baseUrl = `http://127.0.0.1:${port}`;
      const client = yield* Client.makeDenoraClient(baseUrl);
      const health = yield* client.health();
      assert.deepStrictEqual(health, { status: "ok" });
      assert.strictEqual(health.status, "ok");
    }).pipe(Effect.provide(serverLayer)),
  );

  it.effect("exposes the typed endpoint functions on the client", () =>
    Effect.gen(function* () {
      const server = yield* HttpServer.HttpServer;
      const port = server.address._tag === "TcpAddress" ? server.address.port : 0;
      const client = yield* Client.makeDenoraClient(`http://127.0.0.1:${port}`);
      assert.isFunction(client.health);
      assert.isFunction(client.me);
      assert.isFunction(client.archiveConversation);
      assert.isFunction(client.deleteConversation);
      assert.isFunction(client.createAgentRun);
    }).pipe(Effect.provide(serverLayer)),
  );

  it.effect("creates and lists conversations with plain request payloads", () =>
    Effect.gen(function* () {
      const server = yield* HttpServer.HttpServer;
      const port = server.address._tag === "TcpAddress" ? server.address.port : 0;
      const client = yield* Client.makeDenoraClient(`http://127.0.0.1:${port}`);

      const created = yield* client.createConversation({
        payload: { title: "New conversation" },
      });
      assert.strictEqual(created.title, "New conversation");

      const conversations = yield* client.listConversations();
      assert.deepStrictEqual(
        conversations.map((conversation) => conversation.id),
        [created.id],
      );
    }).pipe(Effect.provide(serverLayer)),
  );

  it.effect("creates agent runs with typed stream locations", () =>
    Effect.gen(function* () {
      const server = yield* HttpServer.HttpServer;
      const port = server.address._tag === "TcpAddress" ? server.address.port : 0;
      const client = yield* Client.makeDenoraClient(`http://127.0.0.1:${port}`);
      const runId = ConversationDomain.makeRunId();

      const created = yield* client.createAgentRun({
        payload: { runId, input: { prompt: "hello" } },
      });

      assert.strictEqual(created.runId, runId);
      assert.strictEqual(created.streamPath, `runs/${runId}`);
      assert.strictEqual(new URL(created.streamUrl).pathname, `/runs/${runId}`);
      assert.isString(created.offset);
      assert.isTrue(created.offset.length > 0);
    }).pipe(Effect.provide(serverLayer)),
  );

  it.effect("accepts a custom httpClientLayer and still decodes Health", () =>
    Effect.gen(function* () {
      const server = yield* HttpServer.HttpServer;
      const port = server.address._tag === "TcpAddress" ? server.address.port : 0;
      const client = yield* Client.makeDenoraClient(`http://127.0.0.1:${port}`, {
        httpClientLayer: FetchHttpClient.layer,
      });
      const health = yield* client.health();
      assert.deepStrictEqual(health, { status: "ok" });
    }).pipe(Effect.provide(serverLayer)),
  );
});
