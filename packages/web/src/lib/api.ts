import { Client } from "@denora/server/Client";
import {
  mutationOptions,
  queryOptions,
  type MutationFunctionContext,
  type QueryFunctionContext,
  type QueryKey,
  type UseMutationOptions,
  type UseQueryOptions,
} from "@tanstack/react-query";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as ManagedRuntime from "effect/ManagedRuntime";
import * as Option from "effect/Option";
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/unstable/http";

import { withAuthForwardingHeaders } from "./request-auth-headers";

const rawApiUrl = import.meta.env.VITE_API_URL;

const apiUrl = rawApiUrl?.replace(/\/+$/, "") ?? "";

const withRequestHeaders = HttpClient.mapRequestEffect((request) =>
  Effect.promise(async () => {
    const headers = await withAuthForwardingHeaders(request.headers);
    return HttpClientRequest.setHeaders(request, Object.fromEntries(headers.entries()));
  }),
);

const FetchClientLive = FetchHttpClient.layer.pipe(
  Layer.provide(Layer.succeed(FetchHttpClient.RequestInit)({ credentials: "include" })),
);

const apiRuntime =
  apiUrl.length > 0
    ? ManagedRuntime.make(
        Client.DenoraClient.layer(apiUrl, {
          httpClientLayer: FetchClientLive,
          transformClient: (client) => client.pipe(withRequestHeaders),
        }),
      )
    : undefined;

export type DenoraApiClient = Client.DenoraClient["Service"];

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

export interface HealthResponse {
  readonly status: string;
}

export function apiEffect<A, E>(
  makeEffect: (client: DenoraApiClient) => Effect.Effect<A, E, never>,
): Effect.Effect<A, E, Client.DenoraClient> {
  return Effect.flatMap(Client.DenoraClient, makeEffect);
}

export async function runApi<A, E>(
  effect: Effect.Effect<A, E, Client.DenoraClient>,
  options: ApiRunOptions = {},
): Promise<A> {
  if (!apiRuntime) {
    throw new Error("VITE_API_URL is not configured");
  }

  const runnable = Effect.scoped(
    effect.pipe(Effect.withSpan(options.span ?? "api"), Effect.tapCause(Effect.logError)),
  );
  const exit = await apiRuntime.runPromiseExit(runnable, {
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

const spanFromKey = (key: readonly unknown[] | undefined, fallback: string) => {
  const firstSegment = key?.[0];
  return typeof firstSegment === "string" ? firstSegment : fallback;
};

type ApiQueryFn<TQueryFnData, TError, TQueryKey extends QueryKey> = (input: {
  readonly client: DenoraApiClient;
  readonly context: QueryFunctionContext<TQueryKey>;
}) => Effect.Effect<TQueryFnData, TError, never>;

export type ApiQueryOptions<
  TQueryFnData,
  TError,
  TData = TQueryFnData,
  TQueryKey extends QueryKey = QueryKey,
> = Omit<UseQueryOptions<TQueryFnData, TError | ApiDefect, TData, TQueryKey>, "queryFn"> & {
  readonly queryFn: ApiQueryFn<TQueryFnData, TError, TQueryKey>;
  readonly span?: string | undefined;
};

export function apiQueryOptions<
  TQueryFnData,
  TError = never,
  TData = TQueryFnData,
  TQueryKey extends QueryKey = QueryKey,
>(options: ApiQueryOptions<TQueryFnData, TError, TData, TQueryKey>) {
  const { queryFn, queryKey, span, ...tanstackOptions } = options;

  return queryOptions({
    ...tanstackOptions,
    queryKey,
    queryFn: (context: QueryFunctionContext<TQueryKey>) =>
      runApi(
        apiEffect((client) => queryFn({ client, context })),
        {
          signal: context.signal,
          span: span ?? spanFromKey(queryKey, "api-query"),
        },
      ),
  });
}

type ApiMutationFn<TData, TError, TVariables> = (input: {
  readonly client: DenoraApiClient;
  readonly variables: TVariables;
  readonly context: MutationFunctionContext;
}) => Effect.Effect<TData, TError, never>;

export type ApiMutationOptions<TData, TError, TVariables = void, TOnMutateResult = unknown> = Omit<
  UseMutationOptions<TData, TError | ApiDefect, TVariables, TOnMutateResult>,
  "mutationFn"
> & {
  readonly mutationFn: ApiMutationFn<TData, TError, TVariables>;
  readonly span?: string | undefined;
};

export function apiMutationOptions<
  TData,
  TError = never,
  TVariables = void,
  TOnMutateResult = unknown,
>(options: ApiMutationOptions<TData, TError, TVariables, TOnMutateResult>) {
  const { mutationFn, mutationKey, span, ...tanstackOptions } = options;
  const resolvedMutationFn = (variables: TVariables, context: MutationFunctionContext) =>
    runApi(
      apiEffect((client) => mutationFn({ client, variables, context })),
      {
        span: span ?? spanFromKey(mutationKey, "api-mutation"),
      },
    );

  if (mutationKey === undefined) {
    return mutationOptions({
      ...tanstackOptions,
      mutationFn: resolvedMutationFn,
    });
  }

  return mutationOptions({
    ...tanstackOptions,
    mutationKey,
    mutationFn: resolvedMutationFn,
  });
}
