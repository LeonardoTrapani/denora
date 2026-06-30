import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

/** A host-owned tool bridge callable from sandboxed code by dotted path. */
export interface SandboxToolInvoker {
  readonly invoke: (input: {
    readonly path: string;
    readonly args: unknown;
  }) => Effect.Effect<unknown, unknown>;
}

/** User-visible output accumulated by sandbox helpers. */
export const ExecuteOutputItem = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("file"),
    file: Schema.Unknown,
  }),
  Schema.Struct({
    type: Schema.Literal("content"),
    content: Schema.Unknown,
  }),
]);

export type ExecuteOutputItem = Schema.Schema.Type<typeof ExecuteOutputItem>;

/** Result of executing code in a sandbox. */
export interface ExecuteResult {
  readonly result: unknown;
  readonly output?: readonly ExecuteOutputItem[] | undefined;
  readonly error?: string | undefined;
  readonly logs?: readonly string[] | undefined;
}

export interface ExecuteInput {
  readonly code: string;
  readonly toolInvoker: SandboxToolInvoker;
  readonly timeoutMs?: number | undefined;
  readonly modules?: Record<string, string> | undefined;
  /**
   * `null` blocks outbound fetch/connect from the sandbox. `undefined` uses
   * the sandbox implementation default, which should also be isolated.
   */
  readonly globalOutbound?: unknown | null | undefined;
}

export class CodeCompilationFailed extends Schema.TaggedErrorClass<CodeCompilationFailed>()(
  "CodeSandboxCodeCompilationFailed",
  {
    runtime: Schema.String,
    message: Schema.String,
    cause: Schema.optional(Schema.Unknown),
  },
) {}

export class SandboxRuntimeFailed extends Schema.TaggedErrorClass<SandboxRuntimeFailed>()(
  "CodeSandboxRuntimeFailed",
  {
    runtime: Schema.String,
    message: Schema.String,
    cause: Schema.optional(Schema.Unknown),
  },
) {}

/** Safe, user-facing tool failure that sandboxed code is allowed to observe. */
export class ToolInvocationFailed extends Schema.TaggedErrorClass<ToolInvocationFailed>()(
  "CodeSandboxToolInvocationFailed",
  {
    message: Schema.String,
    cause: Schema.optional(Schema.Unknown),
  },
) {}

export class ExecutionFailed extends Schema.TaggedErrorClass<ExecutionFailed>()(
  "CodeSandboxExecutionFailed",
  {
    message: Schema.String,
    cause: Schema.optional(Schema.Unknown),
  },
) {}

export interface Interface {
  readonly execute: (input: ExecuteInput) => Effect.Effect<ExecuteResult, ExecutionFailed>;
}

export class Service extends Context.Service<Service, Interface>()(
  "@denora/server/code-sandbox/CodeSandbox",
) {}

export * as CodeSandbox from "./CodeSandbox.ts";
