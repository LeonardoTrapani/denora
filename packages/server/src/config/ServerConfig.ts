import * as Config from "effect/Config";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Redacted from "effect/Redacted";

export const DefaultWebOrigins = [
  "http://localhost:1337",
  "http://localhost:1338",
  "http://localhost:1339",
  "http://localhost:1340",
  "http://localhost:1341",
  "http://localhost:3000",
  "http://localhost:5173",
  "http://localhost:8081",
  "http://127.0.0.1:1337",
  "http://127.0.0.1:1338",
  "http://127.0.0.1:1339",
  "http://127.0.0.1:1340",
  "http://127.0.0.1:1341",
  "http://127.0.0.1:3000",
  "http://127.0.0.1:5173",
  "http://127.0.0.1:8081",
] as const;

export const DefaultApiOrigin = "http://localhost:1338";

export interface Auth {
  readonly apiKey: Redacted.Redacted<string>;
  readonly baseURL: string;
  readonly clientId: string;
  readonly cookieDomain: string | undefined;
  readonly cookiePassword: Redacted.Redacted<string>;
  readonly webOrigins: ReadonlyArray<string>;
}

export interface Model {
  readonly openRouterApiKey: Redacted.Redacted<string>;
}

export interface Values {
  readonly auth: Auth;
  readonly model: Model;
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
  Config.withDefault(DefaultApiOrigin),
);

const clientId = Config.string("WORKOS_CLIENT_ID");

const cookieDomain = Config.string("DENORA_COOKIE_DOMAIN").pipe(
  Config.withDefault(""),
  Config.map((value) => value.trim().replace(/^\.+/, "") || undefined),
);

const cookiePassword = Config.redacted("WORKOS_COOKIE_PASSWORD");

const openRouterApiKey = Config.redacted("OPENROUTER_API_KEY");

export const load: Config.Config<Values> = Config.all({
  auth: Config.all({
    apiKey,
    baseURL,
    clientId,
    cookieDomain,
    cookiePassword,
    webOrigins,
  }),
  model: Config.all({
    openRouterApiKey,
  }),
});

export const defaultLayer = Layer.effect(
  Service,
  load.pipe(Effect.map((values) => Service.of(values))),
);

export * as ServerConfig from "./ServerConfig.ts";
