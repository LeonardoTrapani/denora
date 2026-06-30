import type { WorkerLoader } from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";
import { describe, expect, it } from "vitest";
import { CodeSandbox } from "../../src/code-sandbox/CodeSandbox.ts";
import { CloudflareDynamicWorkerSandbox } from "../../src/code-sandbox/CloudflareDynamicWorkerSandbox.ts";

describe("CloudflareDynamicWorkerSandbox host helpers", () => {
  it("dispatches sandbox tool calls through the supplied invoker", async () => {
    const dispatcher = CloudflareDynamicWorkerSandbox.makeToolDispatcher(
      {
        invoke: ({ path, args }) => Effect.succeed({ path, args }),
      },
      Effect.runPromise,
    );

    await expect(dispatcher.call("calendar.events.create", { title: "Review" })).resolves.toEqual({
      ok: true,
      result: {
        path: "calendar.events.create",
        args: { title: "Review" },
      },
    });
  });

  it("exposes public tool invocation failures in the worker RPC error envelope", async () => {
    const dispatcher = CloudflareDynamicWorkerSandbox.makeToolDispatcher(
      {
        invoke: () =>
          Effect.fail(new CodeSandbox.ToolInvocationFailed({ message: "User denied approval" })),
      },
      Effect.runPromise,
    );

    await expect(dispatcher.call("mail.send", { subject: "Hello" })).resolves.toMatchObject({
      ok: false,
      error: {
        kind: "fail",
        message: "User denied approval",
        primary: {
          __type: "Error",
          name: "CodeSandboxToolInvocationFailed",
          message: "User denied approval",
        },
        interrupted: false,
      },
    });
  });

  it("redacts unexpected host tool failures behind an internal correlation message", async () => {
    const dispatcher = CloudflareDynamicWorkerSandbox.makeToolDispatcher(
      {
        invoke: () =>
          Effect.fail(new CodeSandbox.ExecutionFailed({ message: "secret database URL" })),
      },
      Effect.runPromise,
    );

    const response = await dispatcher.call("mail.send", { subject: "Hello" });

    expect(response).toMatchObject({ ok: false });
    if (response.ok) throw new Error("expected failure response");
    expect(response.error.message).toMatch(/^Internal tool error \[[0-9a-f]{8}\]$/);
    expect(response.error.message).not.toContain("secret");
    expect(response.error.primary).toMatchObject({
      __type: "Error",
      name: "CodeSandboxToolInvocationFailed",
      message: response.error.message,
    });
    expect(JSON.stringify(response.error)).not.toContain("secret database URL");
  });

  it("rehydrates Blob/File envelopes before invoking host tools", async () => {
    let captured: unknown;
    const dispatcher = CloudflareDynamicWorkerSandbox.makeToolDispatcher(
      {
        invoke: ({ args }) => {
          captured = args;
          return Effect.succeed("ok");
        },
      },
      Effect.runPromise,
    );
    const blobBuffer = await new Blob(["hello"], { type: "text/plain" }).arrayBuffer();
    const fileBuffer = await new Blob(["file-body"], { type: "text/plain" }).arrayBuffer();
    const bytes = new Uint8Array([0xde, 0xad]);

    await expect(
      dispatcher.call("uploads.send", {
        blob: {
          __executorBinary: 1,
          kind: "blob",
          type: "text/plain",
          buffer: blobBuffer,
        },
        file: {
          __executorBinary: 1,
          kind: "file",
          type: "text/plain",
          name: "report.txt",
          lastModified: 1_700_000_000_000,
          buffer: fileBuffer,
        },
        bytes,
      }),
    ).resolves.toEqual({ ok: true, result: "ok" });

    const record = captured as {
      readonly blob: Blob;
      readonly file: File;
      readonly bytes: Uint8Array;
    };
    expect(record.blob).toBeInstanceOf(Blob);
    expect(record.blob.type).toBe("text/plain");
    await expect(record.blob.text()).resolves.toBe("hello");
    expect(record.file).toBeInstanceOf(File);
    expect(record.file.name).toBe("report.txt");
    expect(record.file.type).toBe("text/plain");
    expect(record.file.lastModified).toBe(1_700_000_000_000);
    await expect(record.file.text()).resolves.toBe("file-body");
    expect(record.bytes).toBe(bytes);
  });

  it("encodes Blob/File tool results before returning the success envelope", async () => {
    const bytes = new Uint8Array([0xca, 0xfe]);
    const dispatcher = CloudflareDynamicWorkerSandbox.makeToolDispatcher(
      {
        invoke: () =>
          Effect.succeed({
            nested: [new Blob(["download"], { type: "text/plain" })],
            file: new File(["named"], "download.txt", {
              type: "text/plain",
              lastModified: 1_700_000_000_001,
            }),
            bytes,
          }),
      },
      Effect.runPromise,
    );

    const response = await dispatcher.call("downloads.fetch", {});

    expect(response).toMatchObject({ ok: true });
    if (!response.ok) throw new Error("expected success response");
    const result = response.result as {
      readonly nested: readonly [
        {
          readonly __executorBinary: 1;
          readonly kind: "blob";
          readonly type: string;
          readonly buffer: ArrayBuffer;
        },
      ];
      readonly file: {
        readonly __executorBinary: 1;
        readonly kind: "file";
        readonly type: string;
        readonly name: string;
        readonly lastModified: number;
        readonly buffer: ArrayBuffer;
      };
      readonly bytes: Uint8Array;
    };
    expect(result.nested[0]).toMatchObject({
      __executorBinary: 1,
      kind: "blob",
      type: "text/plain",
    });
    await expect(new Blob([result.nested[0].buffer]).text()).resolves.toBe("download");
    expect(result.file).toMatchObject({
      __executorBinary: 1,
      kind: "file",
      type: "text/plain",
      name: "download.txt",
      lastModified: 1_700_000_000_001,
    });
    await expect(new Blob([result.file.buffer]).text()).resolves.toBe("named");
    expect(result.bytes).toBe(bytes);
  });

  it("returns a failure envelope for circular RPC args before host invocation", async () => {
    let invoked = false;
    const dispatcher = CloudflareDynamicWorkerSandbox.makeToolDispatcher(
      {
        invoke: () => {
          invoked = true;
          return Effect.succeed(null);
        },
      },
      Effect.runPromise,
    );
    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;

    const response = await dispatcher.call("uploads.send", cyclic);

    expect(response).toMatchObject({ ok: false });
    if (response.ok) throw new Error("expected failure response");
    expect(response.error.message).toMatch(/^Internal tool error \[[0-9a-f]{8}\]$/);
    expect(JSON.stringify(response.error)).not.toContain(
      "Tool RPC payload contains a circular reference",
    );
    expect(invoked).toBe(false);
  });

  it("returns an opaque failure envelope when Blob/File result encoding fails", async () => {
    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;
    const dispatcher = CloudflareDynamicWorkerSandbox.makeToolDispatcher(
      {
        invoke: () => Effect.succeed(cyclic),
      },
      Effect.runPromise,
    );

    const response = await dispatcher.call("downloads.fetch", {});

    expect(response).toMatchObject({ ok: false });
    if (response.ok) throw new Error("expected failure response");
    expect(response.error.message).toMatch(/^Internal tool error \[[0-9a-f]{8}\]$/);
    expect(JSON.stringify(response.error)).not.toContain(
      "Tool RPC payload contains a circular reference",
    );
  });

  it("renders worker error messages from primary serialized errors", () => {
    expect(
      CloudflareDynamicWorkerSandbox.renderWorkerError({
        kind: "unknown",
        message: "fallback",
        primary: { __type: "Error", name: "Error", message: "boom" },
        failures: [],
        defects: [],
        interrupted: false,
      }),
    ).toBe("boom");
  });

  it("classifies compilation, reportable runtime, and internal sandbox failures", () => {
    expect(
      CloudflareDynamicWorkerSandbox.classifySandboxFailure(
        { name: "SyntaxError", message: "Unexpected token ':'" },
        "Unexpected token ':'",
      ),
    ).toBe("compilation");
    expect(
      CloudflareDynamicWorkerSandbox.classifySandboxFailure(
        { name: "DataCloneError", message: "could not be cloned" },
        "could not be cloned",
      ),
    ).toBe("runtime");
    expect(
      CloudflareDynamicWorkerSandbox.classifySandboxFailure(
        { name: "Error", message: "database unavailable" },
        "database unavailable",
      ),
    ).toBe("internal");
  });

  it("uses an injected dispatcher factory seam for worker evaluation", async () => {
    const dispatcher = {
      call: async (path: string, args: unknown) => ({ ok: true as const, result: { path, args } }),
    };
    const loader = {
      Type: "Cloudflare.DynamicWorker" as const,
      name: "CODE_WORKER_LOADER",
      get: () =>
        Effect.succeed({
          fetch: Effect.die("not used"),
          getEntrypoint: () => ({
            evaluate: (received: unknown) =>
              Effect.succeed({ result: received === dispatcher ? "factory-dispatcher" : "wrong" }),
          }),
        }),
    } as unknown as WorkerLoader;
    const sandbox = CloudflareDynamicWorkerSandbox.make({
      loader,
      dispatcherFactory: { make: () => dispatcher },
    });

    await expect(
      Effect.runPromise(
        sandbox.execute({
          code: "return 42;",
          toolInvoker: { invoke: () => Effect.succeed(null) },
        }),
      ),
    ).resolves.toEqual({ result: "factory-dispatcher" });
  });

  it("builds a dynamic worker with blocked outbound networking by default", async () => {
    const loads: unknown[] = [];
    const loader = {
      Type: "Cloudflare.DynamicWorker" as const,
      name: "CODE_WORKER_LOADER",
      get: (_name: string | null, getCode: () => unknown) => {
        loads.push(getCode());
        return Effect.succeed({
          fetch: Effect.die("not used"),
          getEntrypoint: () => ({
            evaluate: () =>
              Effect.succeed({
                result: 42,
                logs: ["ok"],
              }),
          }),
        });
      },
    } as unknown as WorkerLoader;
    const sandbox = CloudflareDynamicWorkerSandbox.make({ loader });

    await expect(
      Effect.runPromise(
        sandbox.execute({
          code: "return 42;",
          toolInvoker: { invoke: () => Effect.succeed(null) },
        }),
      ),
    ).resolves.toEqual({ result: 42, logs: ["ok"] });
    expect(loads).toHaveLength(1);
    expect(loads[0]).toMatchObject({
      compatibilityDate: "2026-01-28",
      compatibilityFlags: ["nodejs_compat"],
      mainModule: "sandbox.js",
      globalOutbound: null,
    });
  });

  it("recovers and strips user code before building the dynamic worker module", async () => {
    const loads: Array<{ readonly modules: Record<string, string> }> = [];
    const loader = {
      Type: "Cloudflare.DynamicWorker" as const,
      name: "CODE_WORKER_LOADER",
      get: (_name: string | null, getCode: () => { readonly modules: Record<string, string> }) => {
        const options = getCode();
        loads.push(options);
        return Effect.succeed({
          fetch: Effect.die("not used"),
          getEntrypoint: () => ({
            evaluate: () => Effect.succeed({ result: "ok" }),
          }),
        });
      },
    } as unknown as WorkerLoader;
    const sandbox = CloudflareDynamicWorkerSandbox.make({ loader });

    await expect(
      Effect.runPromise(
        sandbox.execute({
          code: "```ts\nexport default async (): Promise<number> => 42\n```",
          toolInvoker: { invoke: () => Effect.succeed(null) },
        }),
      ),
    ).resolves.toEqual({ result: "ok" });

    const moduleSource = loads[0]?.modules["sandbox.js"] ?? "";
    expect(moduleSource).toContain("const __fn = (");
    expect(moduleSource).toContain("async () => 42");
    expect(moduleSource).not.toContain(": Promise<number>");
    expect(moduleSource).not.toContain("export default async");
  });

  it("folds user code preparation failures into ExecuteResult.error", async () => {
    const loads: unknown[] = [];
    const loader = {
      Type: "Cloudflare.DynamicWorker" as const,
      name: "CODE_WORKER_LOADER",
      get: (_name: string | null, getCode: () => unknown) => {
        loads.push(getCode());
        return Effect.die("should not load invalid code");
      },
    } as unknown as WorkerLoader;
    const sandbox = CloudflareDynamicWorkerSandbox.make({ loader });

    await expect(
      Effect.runPromise(
        sandbox.execute({
          code: "const = 5;",
          toolInvoker: { invoke: () => Effect.succeed(null) },
        }),
      ),
    ).resolves.toMatchObject({ result: null, error: expect.stringContaining("Unexpected") });
    expect(loads).toHaveLength(0);
  });

  it("folds dynamic worker startup compilation failures into ExecuteResult.error", async () => {
    const loader = {
      Type: "Cloudflare.DynamicWorker" as const,
      name: "CODE_WORKER_LOADER",
      get: () => {
        throw new SyntaxError("Unexpected token ':'");
      },
    } as unknown as WorkerLoader;
    const sandbox = CloudflareDynamicWorkerSandbox.make({ loader });

    await expect(
      Effect.runPromise(
        sandbox.execute({
          code: "return 42;",
          toolInvoker: { invoke: () => Effect.succeed(null) },
        }),
      ),
    ).resolves.toEqual({ result: null, error: "Unexpected token ':'" });
  });

  it("folds reportable evaluate Promise rejections into ExecuteResult.error", async () => {
    const loader = {
      Type: "Cloudflare.DynamicWorker" as const,
      name: "CODE_WORKER_LOADER",
      get: () =>
        Effect.succeed({
          fetch: Effect.die("not used"),
          getEntrypoint: () => ({
            evaluate: () =>
              Promise.reject(new DOMException("could not be cloned", "DataCloneError")),
          }),
        }),
    } as unknown as WorkerLoader;
    const sandbox = CloudflareDynamicWorkerSandbox.make({ loader });

    await expect(
      Effect.runPromise(
        sandbox.execute({
          code: "return Symbol('not cloneable');",
          toolInvoker: { invoke: () => Effect.succeed(null) },
        }),
      ),
    ).resolves.toEqual({ result: null, error: "could not be cloned" });
  });

  it("rejects malformed dynamic worker response envelopes on the failure channel", async () => {
    const loader = {
      Type: "Cloudflare.DynamicWorker" as const,
      name: "CODE_WORKER_LOADER",
      get: () =>
        Effect.succeed({
          fetch: Effect.die("not used"),
          getEntrypoint: () => ({
            evaluate: () => Effect.succeed({ result: "ok", logs: [5] }),
          }),
        }),
    } as unknown as WorkerLoader;
    const sandbox = CloudflareDynamicWorkerSandbox.make({ loader });

    await expect(
      Effect.runPromise(
        sandbox.execute({
          code: "return 42;",
          toolInvoker: { invoke: () => Effect.succeed(null) },
        }),
      ),
    ).rejects.toMatchObject({ _tag: "CodeSandboxExecutionFailed" });
  });

  it("leaves unrecognized sandbox defects on the failure channel", async () => {
    const loader = {
      Type: "Cloudflare.DynamicWorker" as const,
      name: "CODE_WORKER_LOADER",
      get: () =>
        Effect.succeed({
          fetch: Effect.die("not used"),
          getEntrypoint: () => ({
            evaluate: () =>
              Effect.fail(new CodeSandbox.ExecutionFailed({ message: "database unavailable" })),
          }),
        }),
    } as unknown as WorkerLoader;
    const sandbox = CloudflareDynamicWorkerSandbox.make({ loader });

    await expect(
      Effect.runPromise(
        sandbox.execute({
          code: "return 42;",
          toolInvoker: { invoke: () => Effect.succeed(null) },
        }),
      ),
    ).rejects.toMatchObject({ _tag: "CodeSandboxExecutionFailed" });
  });
});
