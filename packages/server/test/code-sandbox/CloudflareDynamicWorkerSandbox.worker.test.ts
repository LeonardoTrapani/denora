import type { WorkerLoader } from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";
import { describe, expect, it } from "vitest";
import { CodeSandbox } from "../../src/code-sandbox/CodeSandbox.ts";
import { CloudflareDynamicWorkerSandbox } from "../../src/code-sandbox/CloudflareDynamicWorkerSandbox.ts";

const liveWorkerLoaderEnabled =
  (globalThis as { readonly process?: { readonly env?: Record<string, string | undefined> } })
    .process?.env?.DENORA_WORKER_LOADER_LIVE === "1";

const makeLiveSandbox = async (): Promise<CodeSandbox.Interface> => {
  const { env } = await import("cloudflare:workers");
  const loader = (env.CODE_WORKER_LOADER ?? env.LOADER) as WorkerLoader | undefined;
  if (!loader) {
    throw new Error("Expected env.CODE_WORKER_LOADER or env.LOADER WorkerLoader binding");
  }

  const { CloudflareRpcToolDispatcher } =
    await import("../../src/code-sandbox/CloudflareRpcToolDispatcher.ts");

  return CloudflareDynamicWorkerSandbox.make({
    loader,
    dispatcherFactory: CloudflareRpcToolDispatcher.dispatcherFactory,
    globalOutbound: null,
    timeoutMs: 1_000,
  });
};

const runLive = async (
  code: string,
  toolInvoker: CodeSandbox.SandboxToolInvoker = { invoke: () => Effect.succeed(null) },
): Promise<CodeSandbox.ExecuteResult> => {
  const sandbox = await makeLiveSandbox();
  return Effect.runPromise(sandbox.execute({ code, toolInvoker, timeoutMs: 1_000 }));
};

describe.skipIf(!liveWorkerLoaderEnabled)(
  "CloudflareDynamicWorkerSandbox live WorkerLoader transport",
  () => {
    it("executes simple code and returns 42", async () => {
      await expect(runLive("return 42;")).resolves.toMatchObject({ result: 42 });
    });

    it("captures console logs", async () => {
      const result = await runLive('console.log("hello"); console.warn("careful"); return 1;');

      expect(result.error).toBeUndefined();
      expect(result.logs).toEqual(expect.arrayContaining(["hello", "[warn] careful"]));
    });

    it("returns thrown errors in ExecuteResult.error", async () => {
      const result = await runLive('throw new Error("boom");');

      expect(result.result).toBeNull();
      expect(result.error).toBe("boom");
    });

    it("dispatches tool calls across worker to host and back", async () => {
      const result = await runLive("return await tools.math.add({ a: 3, b: 4 });", {
        invoke: ({ path, args }) => {
          expect(path).toBe("math.add");
          const input = args as { readonly a: number; readonly b: number };
          return Effect.succeed(input.a + input.b);
        },
      });

      expect(result.error).toBeUndefined();
      expect(result.result).toBe(7);
    });

    it("passes Blob args to host tools as Blob", async () => {
      let captured: unknown;
      const result = await runLive(
        'await tools.upload.send({ file: new Blob(["hello"], { type: "text/plain" }) }); return "ok";',
        {
          invoke: ({ args }) => {
            captured = (args as { readonly file?: unknown }).file;
            return Effect.succeed(null);
          },
        },
      );

      expect(result.error).toBeUndefined();
      expect(captured).toBeInstanceOf(Blob);
      expect((captured as Blob).type).toBe("text/plain");
      await expect((captured as Blob).text()).resolves.toBe("hello");
    });

    it("returns Blob tool results to sandbox code as Blob", async () => {
      const result = await runLive(
        `const blob = await tools.download.fetch({});
       return { isBlob: blob instanceof Blob, type: blob.type, text: await blob.text() };`,
        {
          invoke: () => Effect.succeed(new Blob(["download"], { type: "text/plain" })),
        },
      );

      expect(result.error).toBeUndefined();
      expect(result.result).toEqual({ isBlob: true, type: "text/plain", text: "download" });
    });

    it("blocks outbound fetch when globalOutbound is null", async () => {
      const result = await runLive('await fetch("https://example.com"); return "unexpected";');

      expect(result.result).toBeNull();
      expect(result.error).toBeDefined();
    });

    it("keeps tools proxies non-thenable", async () => {
      const result = await runLive("return tools.foo.then === undefined;");

      expect(result.error).toBeUndefined();
      expect(result.result).toBe(true);
    });

    it("does not leak host tool defect details to sandbox code", async () => {
      const result = await runLive(
        `try {
         await tools.secret.read({});
       } catch (error) {
         return error instanceof Error ? { message: error.message, stack: error.stack } : String(error);
       }`,
        {
          invoke: () =>
            Effect.fail(new CodeSandbox.ExecutionFailed({ message: "secret database URL" })),
        },
      );

      expect(result.error).toBeUndefined();
      expect(result.result).toMatchObject({
        message: expect.stringMatching(/^Internal tool error/),
      });
      expect(JSON.stringify(result.result)).not.toContain("secret database URL");
    });
  },
);
