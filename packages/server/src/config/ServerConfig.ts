import * as Config from "effect/Config";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Redacted from "effect/Redacted";

export const DefaultWebOrigin = "http://localhost:3000";

export interface Auth {
  readonly apiKey: Redacted.Redacted<string>;
  readonly clientId: string;
  readonly cookiePassword: Redacted.Redacted<string>;
  readonly cookieDomain: string | undefined;
  readonly webOrigins: readonly [string, ...Array<string>];
}

export interface Values {
  readonly auth: Auth;
}

export class Service extends Context.Service<Service, Values>()("@denora/server/ServerConfig") {}

export const layer = (values: Values): Layer.Layer<Service> =>
  Layer.succeed(Service, Service.of(values));

const parseWebOrigins = (value: string): readonly [string, ...Array<string>] => {
  const origins = value
    .split(",")
    .map((origin) => origin.trim())
    .filter((origin) => origin.length > 0);

  return origins.length > 0 ? [origins[0]!, ...origins.slice(1)] : [DefaultWebOrigin];
};

const optionalString = (value: string) => {
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
};

export const load = Effect.gen(function* () {
  const apiKey = yield* Config.redacted("WORKOS_API_KEY");
  const clientId = yield* Config.nonEmptyString("WORKOS_CLIENT_ID");
  const cookiePassword = yield* Config.redacted("WORKOS_COOKIE_PASSWORD");
  const cookieDomain = optionalString(
    yield* Config.string("DENORA_COOKIE_DOMAIN").pipe(Config.withDefault("")),
  );
  const webOrigins = parseWebOrigins(
    yield* Config.string("DENORA_WEB_ORIGINS").pipe(Config.withDefault(DefaultWebOrigin)),
  );

  return {
    auth: {
      apiKey,
      clientId,
      cookiePassword,
      cookieDomain,
      webOrigins,
    },
  } satisfies Values;
});

export const defaultLayer = Layer.effect(
  Service,
  load.pipe(Effect.map((values) => Service.of(values))),
);

export * as ServerConfig from "./ServerConfig.ts";
