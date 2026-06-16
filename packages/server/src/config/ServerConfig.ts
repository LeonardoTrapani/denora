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
  readonly secret: Redacted.Redacted<string>;
  readonly baseURL: string;
  readonly webOrigins: ReadonlyArray<string>;
  readonly google: {
    readonly clientId: string;
    readonly clientSecret: Redacted.Redacted<string>;
  };
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

// Better Auth derives signing keys from this; keep it secret and stable.
const secret = Config.redacted("BETTER_AUTH_SECRET");

// The public origin Better Auth serves from (used for cookies + as a trusted
// origin). Normalized to an origin so a trailing path/slash never leaks in.
const baseURL = Config.string("BETTER_AUTH_URL").pipe(Config.map((value) => new URL(value).origin));

const google = Config.all({
  clientId: Config.string("GOOGLE_CLIENT_ID"),
  clientSecret: Config.redacted("GOOGLE_CLIENT_SECRET"),
});

export const load: Config.Config<Values> = Config.all({
  auth: Config.all({
    secret,
    baseURL,
    webOrigins,
    google,
  }),
});

export const defaultLayer = Layer.effect(
  Service,
  load.pipe(Effect.map((values) => Service.of(values))),
);

export * as ServerConfig from "./ServerConfig.ts";
