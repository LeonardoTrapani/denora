import { describe, expect, it } from "vitest";
import { DynamicWorkerModule } from "../../src/code-sandbox/DynamicWorkerModule.ts";

describe("buildDynamicWorkerModule", () => {
  it("produces a WorkerEntrypoint module", () => {
    const module = DynamicWorkerModule.buildDynamicWorkerModule("return 42;", 5_000);

    expect(module).toContain('import { WorkerEntrypoint } from "cloudflare:workers"');
    expect(module).toContain("class CodeSandboxWorker extends WorkerEntrypoint");
    expect(module).toContain("async evaluate(__dispatcher)");
  });

  it("embeds user code inside a timed Promise.race", () => {
    const module = DynamicWorkerModule.buildDynamicWorkerModule("return 42;", 3_000);

    expect(module).toContain("Promise.race");
    expect(module).toContain("(async () => {");
    expect(module).toContain("return 42;");
    expect(module).toContain("})(),");
    expect(module).toContain("Execution timed out after 3000ms");
    expect(module).toContain("setTimeout");
  });

  it("captures console output into logs", () => {
    const module = DynamicWorkerModule.buildDynamicWorkerModule("return 42;", 5_000);

    expect(module).toContain("const __logs = []");
    expect(module).toContain("console.log =");
    expect(module).toContain("console.warn =");
    expect(module).toContain("console.error =");
    expect(module).toContain("logs: __logs");
  });

  it("exposes recursive tools proxy dispatching __dispatcher.call(path, args)", () => {
    const module = DynamicWorkerModule.buildDynamicWorkerModule(
      "return tools.github.repos.get({});",
      5_000,
    );

    expect(module).toContain("const __makeToolsProxy = (path = [])");
    expect(module).toContain("return __makeToolsProxy([...path, String(prop)])");
    expect(module).toContain("const toolPath = path.join('.')");
    expect(module).toContain("const encoded = await __encodeBinary(args[0])");
    expect(module).toContain("await __dispatcher.call(toolPath, encoded)");
    expect(module).toContain("return __decodeBinary(data.result)");
    expect(module).toContain("const tools = __makeToolsProxy()");
  });

  it("returns result/output/logs and serializes thrown errors", () => {
    const module = DynamicWorkerModule.buildDynamicWorkerModule("throw new Error('boom');", 5_000);

    expect(module).toContain("const __outputs = []");
    expect(module).toContain("const emit = (value) =>");
    expect(module).toContain("return { result, output:");
    expect(module).toContain("catch (err)");
    expect(module).toContain("const __serializeThrownError = (err) =>");
    expect(module).toContain("error: __serializeThrownError(err)");
  });

  it("only exposes public tool and opaque internal tool messages", () => {
    const module = DynamicWorkerModule.buildDynamicWorkerModule("return 42;", 5_000);

    expect(module).toContain("const __publicToolErrorMessage = (error) =>");
    expect(module).toContain("value.name === 'CodeSandboxToolInvocationFailed'");
    expect(module).toContain("error.message.startsWith('Internal tool error')");
    expect(module).not.toContain(
      "if (error && typeof error.message === 'string') return error.message;",
    );
    expect(module).not.toContain(
      "if (error && error.primary && typeof error.primary.message === 'string') return error.primary.message;",
    );
    expect(module).not.toContain(
      "if (error && typeof error.primary === 'string') return error.primary;",
    );
  });

  it("includes Blob/File binary encode/decode helpers for tool RPC", () => {
    const module = DynamicWorkerModule.buildDynamicWorkerModule("return 42;", 5_000);

    expect(module).toContain("const __encodeBinary = async");
    expect(module).toContain("const __decodeBinary = (value");
    expect(module).toContain("__executorBinary: 1");
    expect(module).toContain("value instanceof Blob");
    expect(module).toContain("typeof File !== 'undefined' && value instanceof File");
    expect(module).toContain("value instanceof ArrayBuffer || ArrayBuffer.isView(value)");
    expect(module).toContain("Tool RPC payload contains a circular reference");
    expect(module).toContain("new File([value.buffer], value.name");
    expect(module).toContain("new Blob([value.buffer], { type: value.type })");
  });

  it("uses strict ToolFile and MCP content-block predicates for emit output", () => {
    const module = DynamicWorkerModule.buildDynamicWorkerModule("return 42;", 5_000);

    expect(module).toContain("const __isToolFile =");
    expect(module).toContain("value._tag === 'ToolFile'");
    expect(module).toContain("value.encoding === 'base64'");
    expect(module).toContain("const __isMcpTextContentBlock =");
    expect(module).toContain("const __isMcpImageContentBlock =");
    expect(module).toContain("const __isMcpAudioContentBlock =");
    expect(module).toContain("const __isMcpResourceContentBlock =");
    expect(module).toContain("const __isMcpResourceLinkContentBlock =");
    expect(module).toContain("if (__isMcpContentBlock(value))");
    expect(module).not.toContain("typeof value.type === 'string') {");
  });
});
