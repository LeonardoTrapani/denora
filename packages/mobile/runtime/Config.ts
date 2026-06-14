import * as Config from "effect/Config";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

export interface Values {
  readonly apiUrl: string;
}

export class Service extends Context.Service<Service, Values>()("@denora/mobile/Config") {}

const expoEnv = {
  EXPO_PUBLIC_API_URL: process.env.EXPO_PUBLIC_API_URL,
};

const normalizeApiUrl = (value: string) => value.replace(/\/+$/, "");

export const apiUrl = normalizeApiUrl(expoEnv.EXPO_PUBLIC_API_URL ?? "");

export const requireApiUrl = () => {
  if (apiUrl.length === 0) {
    throw new Error("EXPO_PUBLIC_API_URL is not configured");
  }

  return apiUrl;
};

const provider = ConfigProvider.fromEnv({ env: expoEnv });

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const rawApiUrl = yield* Config.string("EXPO_PUBLIC_API_URL").pipe(Config.withDefault(""));
    console.log("[denora:mobile-config-layer] raw api url", rawApiUrl);

    return Service.of({
      apiUrl: normalizeApiUrl(rawApiUrl),
    });
  }),
).pipe(Layer.provide(ConfigProvider.layer(provider)));
