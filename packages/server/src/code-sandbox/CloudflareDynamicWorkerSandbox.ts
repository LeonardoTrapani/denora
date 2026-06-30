import type { WorkerLoader, WorkerLoaderWorkerCode } from "alchemy/Cloudflare";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import {
  CodeSandbox,
  type ExecuteOutputItem,
  type ExecuteResult,
  type SandboxToolInvoker,
} from "./CodeSandbox.ts";
import { prepareUserCode } from "./CodePreparation.ts";
import { buildDynamicWorkerModule, defaultTimeoutMs, entryModule } from "./DynamicWorkerModule.ts";

export interface Options {
  readonly loader: WorkerLoader;
  readonly compatibilityDate?: string | undefined;
  readonly timeoutMs?: number | undefined;
  readonly modules?: Record<string, string> | undefined;
  /**
   * Creates the host-side tool dispatcher passed over Workers RPC.
   * Defaults to a plain object dispatcher so local/fake tests do not need the
   * Cloudflare-only `RpcTarget` runtime module.
   */
  readonly dispatcherFactory?: DispatcherFactory | undefined;
  /** Defaults to `null` so sandbox outbound network is blocked. */
  readonly globalOutbound?: WorkerLoaderWorkerCode["globalOutbound"] | undefined;
}

export type SerializedWorkerErrorValue = unknown;

export interface SerializedWorkerError {
  readonly kind: "fail" | "die" | "interrupt" | "mixed" | "unknown";
  readonly message: string;
  readonly primary: SerializedWorkerErrorValue | null;
  readonly failures: readonly SerializedWorkerErrorValue[];
  readonly defects: readonly SerializedWorkerErrorValue[];
  readonly interrupted: boolean;
}

export type WorkerRpcResponse =
  | {
      readonly ok: true;
      readonly result: unknown;
    }
  | {
      readonly ok: false;
      readonly error: SerializedWorkerError;
    };

export interface SandboxWorkerDispatcher {
  readonly call: (path: string, args: unknown) => Promise<WorkerRpcResponse>;
}

export type RunPromise = <A, E>(effect: Effect.Effect<A, E>) => Promise<A>;

export interface DispatcherFactory {
  readonly make: (input: {
    readonly invoker: SandboxToolInvoker;
    readonly runPromise: RunPromise;
  }) => SandboxWorkerDispatcher;
}

interface SandboxWorkerResponse {
  readonly result: unknown;
  readonly output?: readonly ExecuteOutputItem[] | undefined;
  readonly error?: SerializedWorkerError | undefined;
  readonly logs?: readonly string[] | undefined;
}

type DynamicWorkerEvaluateResult = Effect.Effect<unknown, unknown> | Promise<unknown>;

interface DynamicWorkerEntrypoint {
  readonly evaluate: (dispatcher: SandboxWorkerDispatcher) => DynamicWorkerEvaluateResult;
}

const runtime = "cloudflare-dynamic-worker";
const defaultCompatibilityDate = "2026-01-28";
const minimumTimeoutMs = 100;
const opaqueToolErrorMessage = "Internal tool error";
const compileSignatures = [
  "Failed to start Worker",
  "SyntaxError",
  "Unexpected token",
  "Invalid or unexpected token",
] as const;
const runtimeSignatures = [
  "could not be cloned",
  "does not support serialization",
  "Could not serialize",
  "exceeded CPU",
  "exceeded memory",
  "Too many concurrent dynamic workers",
] as const;

const NormalizedErrorObject = Schema.Struct({
  __type: Schema.Literal("Error"),
  name: Schema.String,
  message: Schema.String,
});

type NormalizedErrorObject = Schema.Schema.Type<typeof NormalizedErrorObject>;

const MessageObject = Schema.Struct({
  message: Schema.String,
});

const NameObject = Schema.Struct({
  name: Schema.String,
});

const BinaryEnvelopeSchema = Schema.Struct({
  __executorBinary: Schema.Literal(1),
  kind: Schema.Literals(["blob", "file"]),
  type: Schema.String,
  name: Schema.optional(Schema.String),
  lastModified: Schema.optional(Schema.Number),
  buffer: Schema.instanceOf(ArrayBuffer),
});

const SerializedWorkerErrorSchema = Schema.Struct({
  kind: Schema.Literals(["fail", "die", "interrupt", "mixed", "unknown"]),
  message: Schema.String,
  primary: Schema.NullOr(Schema.Unknown),
  failures: Schema.Array(Schema.Unknown),
  defects: Schema.Array(Schema.Unknown),
  interrupted: Schema.Boolean,
});

const SandboxWorkerResponseSchema = Schema.Struct({
  result: Schema.Unknown,
  output: Schema.optional(Schema.Array(CodeSandbox.ExecuteOutputItem)),
  error: Schema.optional(SerializedWorkerErrorSchema),
  logs: Schema.optional(Schema.Array(Schema.String)),
});

const decodeNormalizedErrorObject = Schema.decodeUnknownOption(NormalizedErrorObject);
const decodeMessageObject = Schema.decodeUnknownOption(MessageObject);
const decodeNameObject = Schema.decodeUnknownOption(NameObject);
const decodeBinaryEnvelope = Schema.decodeUnknownOption(BinaryEnvelopeSchema);
const decodeSandboxWorkerResponseSchema = Schema.decodeUnknownEffect(SandboxWorkerResponseSchema);

export class BinaryCodecFailed extends Schema.TaggedErrorClass<BinaryCodecFailed>()(
  "CodeSandboxBinaryCodecFailed",
  {
    operation: Schema.Literals(["decode", "encode"]),
    message: Schema.String,
    cause: Schema.optional(Schema.Unknown),
  },
) {}

const normalizeErrorObject = (error: Error) => ({
  __type: "Error" as const,
  name: error.name,
  message: error.message,
});

const getNormalizedErrorObject = (value: unknown): NormalizedErrorObject | undefined =>
  Option.getOrUndefined(decodeNormalizedErrorObject(value));

const getMessageObject = (value: unknown): { readonly message: string } | undefined =>
  Option.getOrUndefined(decodeMessageObject(value));

const getNameObject = (value: unknown): { readonly name: string } | undefined =>
  Option.getOrUndefined(decodeNameObject(value));

export const serializeWorkerErrorValue = (value: unknown): SerializedWorkerErrorValue => {
  if (value instanceof Error) {
    return normalizeErrorObject(value);
  }

  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean"
  ) {
    return value;
  }

  try {
    return JSON.parse(JSON.stringify(value)) as SerializedWorkerErrorValue;
  } catch {
    return String(value);
  }
};

const renderTransportMessage = (value: unknown): string => {
  if (typeof value === "string") {
    return value;
  }

  const normalized = getNormalizedErrorObject(value);
  if (normalized) {
    return normalized.message;
  }

  const messageObject = getMessageObject(value);
  if (messageObject) {
    return messageObject.message;
  }

  if (typeof value === "object" && value !== null) {
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }

  if (typeof value === "undefined") {
    return "Unknown error";
  }

  return String(value);
};

export const serializeWorkerCause = (cause: Cause.Cause<unknown>): SerializedWorkerError => {
  const failures = cause.reasons
    .filter(Cause.isFailReason)
    .map((reason) => serializeWorkerErrorValue(reason.error));
  const defects = cause.reasons
    .filter(Cause.isDieReason)
    .map((reason) => serializeWorkerErrorValue(reason.defect));
  const interrupted = cause.reasons.some(Cause.isInterruptReason);
  const primary = failures[0] ?? defects[0] ?? null;
  const kind =
    failures.length > 0 && defects.length > 0
      ? "mixed"
      : failures.length > 0
        ? "fail"
        : defects.length > 0
          ? "die"
          : interrupted
            ? "interrupt"
            : "unknown";

  return {
    kind,
    message:
      primary !== null
        ? renderTransportMessage(primary)
        : interrupted
          ? "Interrupted"
          : "Unknown error",
    primary,
    failures,
    defects,
    interrupted,
  };
};

export const renderWorkerError = (error: SerializedWorkerError): string => {
  const normalized = getNormalizedErrorObject(error.primary);
  if (normalized) {
    return normalized.message;
  }

  if (typeof error.primary === "string") {
    return error.primary;
  }

  const messageObject = getMessageObject(error.primary);
  if (messageObject) {
    return messageObject.message;
  }

  if (typeof error.primary === "object" && error.primary !== null) {
    try {
      return JSON.stringify(error.primary);
    } catch {
      return error.message;
    }
  }

  return error.message;
};

export type BinaryEnvelope = Schema.Schema.Type<typeof BinaryEnvelopeSchema>;

const getBinaryEnvelope = (value: unknown): BinaryEnvelope | undefined =>
  Option.getOrUndefined(decodeBinaryEnvelope(value));

const isPlainObject = (value: object): boolean => {
  const proto = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
};

export const rehydrateBinary = (value: unknown, seen = new WeakSet<object>()): unknown => {
  if (value === null || typeof value !== "object") return value;
  if (value instanceof ArrayBuffer || ArrayBuffer.isView(value)) return value;
  if (seen.has(value)) {
    throw new Error("Tool RPC payload contains a circular reference");
  }
  seen.add(value);
  const envelope = getBinaryEnvelope(value);
  if (envelope) {
    seen.delete(value);
    if (envelope.kind === "file" && typeof envelope.name === "string") {
      return new File([envelope.buffer], envelope.name, {
        type: envelope.type,
        ...(typeof envelope.lastModified === "number"
          ? { lastModified: envelope.lastModified }
          : {}),
      });
    }
    return new Blob([envelope.buffer], { type: envelope.type });
  }
  if (Array.isArray(value)) {
    const out = value.map((item) => rehydrateBinary(item, seen));
    seen.delete(value);
    return out;
  }
  if (!isPlainObject(value)) {
    seen.delete(value);
    return value;
  }
  const out: Record<string, unknown> = {};
  for (const [key, item] of Object.entries(value)) {
    out[key] = rehydrateBinary(item, seen);
  }
  seen.delete(value);
  return out;
};

export const encodeBinary = async (
  value: unknown,
  seen = new WeakSet<object>(),
): Promise<unknown> => {
  if (value === null || typeof value !== "object") return value;
  if (value instanceof ArrayBuffer || ArrayBuffer.isView(value)) return value;
  if (seen.has(value)) {
    throw new Error("Tool RPC payload contains a circular reference");
  }
  seen.add(value);
  if (typeof File !== "undefined" && value instanceof File) {
    const out = {
      __executorBinary: 1 as const,
      kind: "file" as const,
      type: value.type,
      name: value.name,
      lastModified: value.lastModified,
      buffer: await value.arrayBuffer(),
    };
    seen.delete(value);
    return out;
  }
  if (value instanceof Blob) {
    const out = {
      __executorBinary: 1 as const,
      kind: "blob" as const,
      type: value.type,
      buffer: await value.arrayBuffer(),
    };
    seen.delete(value);
    return out;
  }
  if (Array.isArray(value)) {
    const out = await Promise.all(value.map((item) => encodeBinary(item, seen)));
    seen.delete(value);
    return out;
  }
  if (!isPlainObject(value)) {
    seen.delete(value);
    return value;
  }
  const out: Record<string, unknown> = {};
  for (const [key, item] of Object.entries(value)) {
    out[key] = await encodeBinary(item, seen);
  }
  seen.delete(value);
  return out;
};

const serializedErrorName = (value: SerializedWorkerErrorValue): string | null =>
  getNameObject(value)?.name ?? null;

export type SandboxFailureKind = "compilation" | "runtime" | "internal";

export const classifySandboxFailure = (
  serialized: SerializedWorkerErrorValue,
  message: string,
): SandboxFailureKind => {
  const name = serializedErrorName(serialized);
  if (
    name === "SyntaxError" ||
    compileSignatures.some((signature) => message.includes(signature))
  ) {
    return "compilation";
  }
  if (
    name === "DataCloneError" ||
    runtimeSignatures.some((signature) => message.includes(signature))
  ) {
    return "runtime";
  }
  return "internal";
};

const codeCompilationFailed = (cause: unknown): CodeSandbox.CodeCompilationFailed => {
  const message = renderTransportMessage(serializeWorkerErrorValue(cause));
  return new CodeSandbox.CodeCompilationFailed({ runtime, message, cause });
};

const toSandboxFailure = (
  cause: unknown,
):
  | CodeSandbox.CodeCompilationFailed
  | CodeSandbox.SandboxRuntimeFailed
  | CodeSandbox.ExecutionFailed => {
  const serialized = serializeWorkerErrorValue(cause);
  const message = renderTransportMessage(serialized);

  switch (classifySandboxFailure(serialized, message)) {
    case "compilation":
      return new CodeSandbox.CodeCompilationFailed({ runtime, message, cause });
    case "runtime":
      return new CodeSandbox.SandboxRuntimeFailed({ runtime, message, cause });
    default:
      return new CodeSandbox.ExecutionFailed({ message, cause });
  }
};

const workerResponseDecodeFailed = (cause: unknown): CodeSandbox.ExecutionFailed =>
  new CodeSandbox.ExecutionFailed({
    message: renderTransportMessage(serializeWorkerErrorValue(cause)),
    cause,
  });

const decodeSandboxWorkerResponse = Effect.fn(
  "CloudflareDynamicWorkerSandbox.decodeSandboxWorkerResponse",
)(function* (
  response: unknown,
): Effect.fn.Return<SandboxWorkerResponse, CodeSandbox.ExecutionFailed> {
  return yield* decodeSandboxWorkerResponseSchema(response).pipe(
    Effect.mapError(workerResponseDecodeFailed),
  );
});

const binaryCodecFailed = (operation: BinaryCodecFailed["operation"], cause: unknown) =>
  new BinaryCodecFailed({
    operation,
    message: renderTransportMessage(serializeWorkerErrorValue(cause)),
    cause,
  });

const decodeToolArgs = Effect.fn("CloudflareDynamicWorkerSandbox.decodeToolArgs")(function* (
  args: unknown,
): Effect.fn.Return<unknown, BinaryCodecFailed> {
  return yield* Effect.try({
    try: () => rehydrateBinary(args),
    catch: (cause) => binaryCodecFailed("decode", cause),
  });
});

const encodeToolResult = Effect.fn("CloudflareDynamicWorkerSandbox.encodeToolResult")(function* (
  value: unknown,
): Effect.fn.Return<WorkerRpcResponse, BinaryCodecFailed> {
  const result = yield* Effect.tryPromise({
    try: () => encodeBinary(value),
    catch: (cause) => binaryCodecFailed("encode", cause),
  });
  return { ok: true, result };
});

const isToolInvocationFailed = (value: unknown): value is CodeSandbox.ToolInvocationFailed =>
  value instanceof CodeSandbox.ToolInvocationFailed;

const publicToolFailureCause = (cause: Cause.Cause<unknown>): boolean => {
  const failures = cause.reasons.filter(Cause.isFailReason).map((reason) => reason.error);
  const hasDefects = cause.reasons.some(Cause.isDieReason);

  return failures.length > 0 && !hasDefects && failures.every(isToolInvocationFailed);
};

const newCorrelationId = (): string =>
  Math.floor(Math.random() * 0x1_0000_0000)
    .toString(16)
    .padStart(8, "0");

const opaqueToolFailure = (correlationId: string): SerializedWorkerError =>
  serializeWorkerCause(
    Cause.fail(
      new CodeSandbox.ToolInvocationFailed({
        message: `${opaqueToolErrorMessage} [${correlationId}]`,
      }),
    ),
  );

const opaqueToolFailureResponse = (
  path: string,
  cause: Cause.Cause<unknown>,
): Effect.Effect<WorkerRpcResponse> => {
  const correlationId = newCorrelationId();

  return Effect.logError("code sandbox tool dispatch failed", cause).pipe(
    Effect.annotateLogs({
      "denora.code_sandbox.correlation_id": correlationId,
      "denora.code_sandbox.tool_path": path,
    }),
    Effect.as({
      ok: false,
      error: opaqueToolFailure(correlationId),
    } as const),
  );
};

export const makeToolDispatcher = (
  invoker: SandboxToolInvoker,
  runPromise: RunPromise,
): SandboxWorkerDispatcher => ({
  call: (path, args) =>
    runPromise(
      decodeToolArgs(args).pipe(
        Effect.flatMap((decodedArgs) => invoker.invoke({ path, args: decodedArgs })),
        Effect.flatMap(encodeToolResult),
        Effect.catchCause((cause) =>
          publicToolFailureCause(cause)
            ? Effect.succeed<WorkerRpcResponse>({
                ok: false,
                error: serializeWorkerCause(cause),
              })
            : opaqueToolFailureResponse(path, cause),
        ),
        Effect.withSpan("denora.code_sandbox.tool_dispatch", {
          attributes: {
            "denora.code_sandbox.tool_path": path,
          },
        }),
      ),
    ),
});

export const plainDispatcherFactory: DispatcherFactory = {
  make: ({ invoker, runPromise }) => makeToolDispatcher(invoker, runPromise),
};

const normalizeExecuteResult = (response: SandboxWorkerResponse): ExecuteResult => {
  const error = response.error ? renderWorkerError(response.error) : undefined;

  return {
    result: error === undefined ? response.result : null,
    ...(response.output && response.output.length > 0 ? { output: response.output } : {}),
    ...(error === undefined ? {} : { error }),
    ...(response.logs ? { logs: response.logs } : {}),
  };
};

const runEvaluateResult = (
  evaluateResult: DynamicWorkerEvaluateResult,
): Effect.Effect<
  SandboxWorkerResponse,
  CodeSandbox.CodeCompilationFailed | CodeSandbox.SandboxRuntimeFailed | CodeSandbox.ExecutionFailed
> =>
  (Effect.isEffect(evaluateResult)
    ? evaluateResult.pipe(Effect.mapError(toSandboxFailure))
    : Effect.tryPromise({
        try: () => evaluateResult,
        catch: toSandboxFailure,
      })
  ).pipe(Effect.flatMap(decodeSandboxWorkerResponse));

const makeExecute = (options: Options): CodeSandbox.Interface["execute"] =>
  Effect.fn("CloudflareDynamicWorkerSandbox.execute")(
    function* (input: CodeSandbox.ExecuteInput) {
      const timeoutMs = Math.max(
        minimumTimeoutMs,
        input.timeoutMs ?? options.timeoutMs ?? defaultTimeoutMs,
      );
      yield* Effect.annotateCurrentSpan({
        "denora.code_sandbox.runtime": runtime,
        "denora.code_sandbox.timeout_ms": timeoutMs,
        "denora.code_sandbox.extra_modules": new Set([
          ...Object.keys(options.modules ?? {}),
          ...Object.keys(input.modules ?? {}),
        ]).size,
      });

      const context = yield* Effect.context<never>();
      const dispatcherFactory = options.dispatcherFactory ?? plainDispatcherFactory;
      const dispatcher = dispatcherFactory.make({
        invoker: input.toolInvoker,
        runPromise: Effect.runPromiseWith(context),
      });
      const preparedCode = yield* Effect.try({
        try: () => prepareUserCode(input.code),
        catch: codeCompilationFailed,
      });
      const moduleSource = buildDynamicWorkerModule(preparedCode, timeoutMs);
      const { [entryModule]: _reservedEntryModule, ...safeOptionModules } = options.modules ?? {};
      const { [entryModule]: _reservedInputModule, ...safeInputModules } = input.modules ?? {};
      const workerOptions = {
        compatibilityDate: options.compatibilityDate ?? defaultCompatibilityDate,
        compatibilityFlags: ["nodejs_compat"],
        mainModule: entryModule,
        modules: {
          ...safeOptionModules,
          ...safeInputModules,
          [entryModule]: moduleSource,
        },
        globalOutbound: (input.globalOutbound ?? options.globalOutbound ?? null) as Exclude<
          WorkerLoaderWorkerCode["globalOutbound"],
          undefined
        >,
      } satisfies WorkerLoaderWorkerCode;
      const workerEffect = yield* Effect.try({
        try: () => options.loader.get(`code-sandbox-${crypto.randomUUID()}`, () => workerOptions),
        catch: toSandboxFailure,
      });
      const worker = yield* workerEffect.pipe(Effect.mapError(toSandboxFailure));
      const entrypoint = worker.getEntrypoint<DynamicWorkerEntrypoint>();
      const evaluateResult = yield* Effect.try({
        try: () => entrypoint.evaluate(dispatcher),
        catch: toSandboxFailure,
      });
      const response = yield* runEvaluateResult(evaluateResult);
      return normalizeExecuteResult(response);
    },
    Effect.catchTags({
      CodeSandboxCodeCompilationFailed: (error) =>
        Effect.succeed({ result: null, error: error.message } satisfies ExecuteResult),
      CodeSandboxRuntimeFailed: (error) =>
        Effect.succeed({ result: null, error: error.message } satisfies ExecuteResult),
    }),
  );

export const make = (options: Options): CodeSandbox.Interface => ({
  execute: makeExecute(options),
});

export const layer = (options: Options): Layer.Layer<CodeSandbox.Service> =>
  Layer.succeed(CodeSandbox.Service, CodeSandbox.Service.of(make(options)));

export * as CloudflareDynamicWorkerSandbox from "./CloudflareDynamicWorkerSandbox.ts";
