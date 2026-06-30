// oxlint-disable-next-line typescript/triple-slash-reference -- scoped ambient module for Cloudflare-only runtime import
/// <reference path="./CloudflareWorkersRuntime.d.ts" />

import { RpcTarget } from "cloudflare:workers";
import type { SandboxToolInvoker } from "./CodeSandbox.ts";
import {
  makeToolDispatcher,
  type DispatcherFactory,
  type RunPromise,
  type SandboxWorkerDispatcher,
  type WorkerRpcResponse,
} from "./CloudflareDynamicWorkerSandbox.ts";

/**
 * Cloudflare Workers RPC target used by live WorkerLoader sandboxes.
 *
 * Keep this module isolated: importing `cloudflare:workers` is only valid in a
 * Cloudflare Worker runtime. Local tests should inject the plain dispatcher
 * factory from `CloudflareDynamicWorkerSandbox` instead.
 */
export class RpcToolDispatcher extends RpcTarget implements SandboxWorkerDispatcher {
  readonly #delegate: SandboxWorkerDispatcher;

  constructor(input: { readonly invoker: SandboxToolInvoker; readonly runPromise: RunPromise }) {
    super();
    this.#delegate = makeToolDispatcher(input.invoker, input.runPromise);
  }

  call(path: string, args: unknown): Promise<WorkerRpcResponse> {
    return this.#delegate.call(path, args);
  }
}

export const make = (input: {
  readonly invoker: SandboxToolInvoker;
  readonly runPromise: RunPromise;
}): RpcToolDispatcher => new RpcToolDispatcher(input);

export const dispatcherFactory: DispatcherFactory = { make };

export * as CloudflareRpcToolDispatcher from "./CloudflareRpcToolDispatcher.ts";
