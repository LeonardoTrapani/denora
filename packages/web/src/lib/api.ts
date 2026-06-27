import * as ClientApi from "@denora/server/client-api";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as ManagedRuntime from "effect/ManagedRuntime";
import * as Option from "effect/Option";
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/unstable/http";

import { WebConfig } from "./WebConfig.ts";
import { Auth } from "./Auth.ts";

const withRequestHeaders = HttpClient.mapRequestEffect((request) =>
  Effect.promise(async () => {
    const headers = await Auth.withAuthForwardingHeaders(request.headers);
    return HttpClientRequest.setHeaders(request, Object.fromEntries(headers.entries()));
  }),
);

const FetchClientLive = FetchHttpClient.layer.pipe(
  Layer.provide(Layer.succeed(FetchHttpClient.RequestInit)({ credentials: "include" })),
);

export const clientLayer = ClientApi.layer(WebConfig.apiUrl || "http://localhost", {
  httpClientLayer: FetchClientLive,
  transformClient: (client) => client.pipe(withRequestHeaders),
});

const managedApiRuntime =
  WebConfig.apiUrl.length > 0 ? ManagedRuntime.make(clientLayer) : undefined;

export type DenoraApiClient = ClientApi.DenoraClient["Service"];

export type ApiRunOptions = {
  readonly signal?: AbortSignal | undefined;
  readonly span?: string | undefined;
};

export class ApiDefect extends Error {
  override readonly name = "ApiDefect";
  override readonly cause: unknown;

  constructor(cause: unknown) {
    super("Denora API request defect", { cause });
    this.cause = cause;
  }
}

export function apiEffect<A, E>(
  makeEffect: (client: DenoraApiClient) => Effect.Effect<A, E, never>,
): Effect.Effect<A, E, ClientApi.DenoraClient> {
  return Effect.flatMap(ClientApi.DenoraClient, makeEffect);
}

export async function runApi<A, E>(
  effect: Effect.Effect<A, E, ClientApi.DenoraClient>,
  options: ApiRunOptions = {},
): Promise<A> {
  const runtime = managedApiRuntime ?? WebConfig.missingApiUrl();

  const runnable = Effect.scoped(
    effect.pipe(Effect.withSpan(options.span ?? "api"), Effect.tapCause(Effect.logError)),
  );
  const exit = await runtime.runPromiseExit(runnable, {
    signal: options.signal,
  });

  if (Exit.isSuccess(exit)) {
    return exit.value;
  }

  const errorOption = Cause.findErrorOption(exit.cause);
  if (Option.isSome(errorOption)) {
    throw errorOption.value;
  }

  throw new ApiDefect(Cause.squash(exit.cause));
}

export * as Api from "./api.ts";
