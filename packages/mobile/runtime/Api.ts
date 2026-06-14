import * as ClientApi from "@denora/server/client-api";
import { SessionCookieName } from "@denora/server/client-api";
import * as SecureStore from "expo-secure-store";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/unstable/http";
import * as MobileConfig from "./Config.ts";

export const SessionStorageKey = "denora.workos.sealed-session";

export type DenoraApiClient = ClientApi.DenoraClient["Service"];

export class MissingApiUrl extends Error {
  override readonly name = "MissingApiUrl";

  constructor() {
    super("EXPO_PUBLIC_API_URL is not configured");
  }
}

export const getSession = () => SecureStore.getItemAsync(SessionStorageKey);

export const setSession = (session: string) => SecureStore.setItemAsync(SessionStorageKey, session);

export const clearSession = () => SecureStore.deleteItemAsync(SessionStorageKey);

export const makeSessionCookie = (session: string) => `${SessionCookieName}=${session}`;

const withStoredSessionCookie = HttpClient.mapRequestEffect((request) =>
  Effect.promise(async () => {
    const session = await getSession();
    if (!session) return request;

    return HttpClientRequest.setHeader(request, "cookie", makeSessionCookie(session));
  }),
);

export const layer = Layer.effect(
  ClientApi.DenoraClient,
  Effect.gen(function* () {
    const config = yield* MobileConfig.Service;
    console.log("[denora:mobile-api-layer] config", { apiUrl: config.apiUrl });
    if (config.apiUrl.length === 0) {
      console.log("[denora:mobile-api-layer] missing api url");
      return yield* Effect.fail(new MissingApiUrl());
    }

    const client = yield* ClientApi.makeDenoraClient(config.apiUrl, {
      transformClient: (client) => client.pipe(withStoredSessionCookie),
      httpClientLayer: FetchHttpClient.layer,
    });
    console.log("[denora:mobile-api-layer] client ready");
    return client;
  }),
).pipe(Layer.provide(MobileConfig.layer));
