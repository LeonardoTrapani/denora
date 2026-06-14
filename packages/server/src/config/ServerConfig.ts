import * as Config from "effect/Config";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";

export const DefaultWebOrigins = ["http://localhost:3000", "http://localhost:8081"] as const;
export const DefaultAppRedirectScheme = "denora";

export interface Auth {
  readonly apiKey: Redacted.Redacted<string>;
  readonly clientId: string;
  readonly csrfSecret: Redacted.Redacted<string>;
  readonly cookiePassword: Redacted.Redacted<string>;
  readonly cookieDomain: string | undefined;
  readonly appRedirectSchemes: ReadonlyArray<string>;
  readonly webOrigins: ReadonlyArray<string>;
}

export interface Values {
  readonly auth: Auth;
}

export class Service extends Context.Service<Service, Values>()("@denora/server/ServerConfig") {}

export const layer = (values: Values): Layer.Layer<Service> =>
  Layer.succeed(Service, Service.of(values));

const cookieDomain = Config.schema(Schema.Trim, "DENORA_COOKIE_DOMAIN").pipe(
  Config.map((value) => (value.length > 0 ? value : undefined)),
  Config.withDefault(undefined),
);

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

const appRedirectSchemes = Config.schema(
  Config.Array(Schema.Trim),
  "DENORA_APP_REDIRECT_SCHEMES",
).pipe(
  Config.map((values) => values.filter((value) => value.length > 0)),
  Config.withDefault([DefaultAppRedirectScheme]),
);

const csrfSecret = Config.schema(Schema.Redacted(Schema.NonEmptyString), "CSRF_SECRET");

const workOsCookiePassword = Config.schema(
  Schema.Redacted(Schema.String.check(Schema.isLengthBetween(32, 32))),
  "WORKOS_COOKIE_PASSWORD",
);

export const load: Config.Config<Values> = Config.all({
  auth: Config.all({
    apiKey: Config.redacted("WORKOS_API_KEY"),
    appRedirectSchemes,
    clientId: Config.nonEmptyString("WORKOS_CLIENT_ID"),
    csrfSecret,
    cookiePassword: workOsCookiePassword,
    cookieDomain,
    webOrigins,
  }),
});

export const defaultLayer = Layer.effect(
  Service,
  load.pipe(Effect.map((values) => Service.of(values))),
);

export * as ServerConfig from "./ServerConfig.ts";
