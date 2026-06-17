import * as Config from "effect/Config";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Redacted from "effect/Redacted";

export const DefaultWebOrigins = [
  "http://localhost:3000",
  "http://localhost:1338",
  "http://localhost:8081",
] as const;

export interface Auth {
  readonly apiKey: Redacted.Redacted<string>;
  readonly baseURL: string;
  readonly clientId: string;
  readonly cookiePassword: Redacted.Redacted<string>;
  readonly webOrigins: ReadonlyArray<string>;
}

export interface Values {
  readonly auth: Auth;
}

export class Service extends Context.Service<Service, Values>()("@denora/server/ServerConfig") {}

export const layer = (values: Values): Layer.Layer<Service> =>
  Layer.succeed(Service, Service.of(values));

const webOrigins = Config.string("DENORA_WEB_ORIGINS").pipe(
  Config.map((value) => {
    const origins = value
      .split(",")
      .map((origin) => origin.trim())
      .filter(Boolean);

    return origins.length > 0 ? origins : [...DefaultWebOrigins];
  }),
  Config.withDefault([...DefaultWebOrigins]),
);

const apiKey = Config.redacted("WORKOS_API_KEY");

// The public origin this API serves from. WorkOS redirects back here and cookies
// are scoped to this origin.
const baseURL = Config.string("WORKOS_REDIRECT_BASE_URL").pipe(
  Config.map((value) => new URL(value).origin),
);

const clientId = Config.string("WORKOS_CLIENT_ID");

const cookiePassword = Config.redacted("WORKOS_COOKIE_PASSWORD");

export const load: Config.Config<Values> = Config.all({
  auth: Config.all({
    apiKey,
    baseURL,
    clientId,
    cookiePassword,
    webOrigins,
  }),
});

export const defaultLayer = Layer.effect(
  Service,
  load.pipe(Effect.map((values) => Service.of(values))),
);

export * as ServerConfig from "./ServerConfig.ts";
