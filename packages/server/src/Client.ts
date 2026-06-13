import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { FetchHttpClient, HttpClient } from "effect/unstable/http";
import * as HttpApiClient from "effect/unstable/httpapi/HttpApiClient";
import { DenoraApi } from "./Api.ts";

export interface DenoraClientOptions {
  readonly transformClient?: ((client: HttpClient.HttpClient) => HttpClient.HttpClient) | undefined;
  readonly httpClientLayer?: Layer.Layer<HttpClient.HttpClient> | undefined;
}

export const makeDenoraClient = (baseUrl: string | URL, options: DenoraClientOptions = {}) =>
  HttpApiClient.make(DenoraApi, {
    baseUrl,
    transformClient: options.transformClient,
  }).pipe(Effect.provide(options.httpClientLayer ?? FetchHttpClient.layer));

export const makeDenoraUrlBuilder = (baseUrl: string | URL) =>
  HttpApiClient.urlBuilder(DenoraApi, { baseUrl });

export class DenoraClient extends Context.Service<
  DenoraClient,
  HttpApiClient.ForApi<typeof DenoraApi>
>()("@denora/server/Client") {
  static readonly layer = (baseUrl: string | URL, options: DenoraClientOptions = {}) =>
    Layer.effect(DenoraClient, makeDenoraClient(baseUrl, options));
}

export * as Client from "./Client.ts";
