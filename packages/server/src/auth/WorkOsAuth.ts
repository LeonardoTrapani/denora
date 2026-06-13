import { WorkOS } from "@workos-inc/node";
import type { User as WorkOsUser } from "@workos-inc/node";
import type * as Alchemy from "alchemy";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as HttpEffect from "effect/unstable/http/HttpEffect";
import type * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import { DefaultWebOrigin, ServerConfig } from "../config/ServerConfig.ts";
import { Db } from "../persistence/Db.ts";
import { schema } from "../persistence/schema.ts";
import { AuthUser } from "./User.ts";

declare const crypto: { randomUUID(): string };

type UserRow = typeof schema.users.$inferSelect;

export const SessionCookieName = "wos-session";

type Options = ServerConfig.Auth;

export interface Runtime {
  readonly options: Options;
  readonly workos: WorkOS;
}

export interface Interface {
  readonly runtime: Runtime;
  readonly login: (
    request: HttpServerRequest.HttpServerRequest,
  ) => Effect.Effect<HttpServerResponse.HttpServerResponse>;
  readonly callback: (
    request: HttpServerRequest.HttpServerRequest,
  ) => Effect.Effect<HttpServerResponse.HttpServerResponse, never, Alchemy.RuntimeContext>;
  readonly logout: (
    request: HttpServerRequest.HttpServerRequest,
  ) => Effect.Effect<HttpServerResponse.HttpServerResponse>;
  readonly authenticateSession: <E>(
    httpEffect: Effect.Effect<HttpServerResponse.HttpServerResponse, E, AuthUser.CurrentUser>,
    credential: Redacted.Redacted<string>,
  ) => Effect.Effect<
    HttpServerResponse.HttpServerResponse,
    E | AuthUser.Unauthorized,
    Alchemy.RuntimeContext | HttpRouter.Provided
  >;
}

export class Service extends Context.Service<Service, Interface>()("@denora/server/WorkOsAuth") {}

const cookieOptions = (options: Options) => ({
  domain: options.cookieDomain,
  path: "/",
  httpOnly: true,
  secure: true,
  sameSite: "lax" as const,
});

const toDenoraUser = (row: UserRow) =>
  new AuthUser.DenoraUser({
    id: row.id,
    workosUserId: row.workosUserId,
    email: row.email,
    emailVerified: row.emailVerified,
    name: row.name,
    firstName: row.firstName,
    lastName: row.lastName,
    profilePictureUrl: row.profilePictureUrl,
    locale: row.locale,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
  });

const syncUser = (client: Db.Client, workosUser: WorkOsUser) =>
  Effect.gen(function* () {
    const now = new Date().toISOString();
    const rows = yield* client
      .insert(schema.users)
      .values({
        id: crypto.randomUUID(),
        workosUserId: workosUser.id,
        email: workosUser.email,
        emailVerified: workosUser.emailVerified,
        name: workosUser.name,
        firstName: workosUser.firstName,
        lastName: workosUser.lastName,
        profilePictureUrl: workosUser.profilePictureUrl,
        locale: workosUser.locale,
        lastSignInAt: workosUser.lastSignInAt,
        workosCreatedAt: workosUser.createdAt,
        workosUpdatedAt: workosUser.updatedAt,
        createdAt: now,
        updatedAt: now,
      })
      .onConflictDoUpdate({
        target: schema.users.workosUserId,
        set: {
          email: workosUser.email,
          emailVerified: workosUser.emailVerified,
          name: workosUser.name,
          firstName: workosUser.firstName,
          lastName: workosUser.lastName,
          profilePictureUrl: workosUser.profilePictureUrl,
          locale: workosUser.locale,
          lastSignInAt: workosUser.lastSignInAt,
          workosCreatedAt: workosUser.createdAt,
          workosUpdatedAt: workosUser.updatedAt,
          updatedAt: now,
        },
      })
      .returning()
      .pipe(Effect.orDie) as Effect.Effect<ReadonlyArray<UserRow>, never, Alchemy.RuntimeContext>;

    return toDenoraUser(rows[0]!);
  });

const setSessionCookie = (
  response: HttpServerResponse.HttpServerResponse,
  value: string,
  options: Options,
) => HttpServerResponse.setCookie(response, SessionCookieName, value, cookieOptions(options));

const clearSessionCookie = (response: HttpServerResponse.HttpServerResponse, options: Options) =>
  HttpServerResponse.expireCookie(response, SessionCookieName, cookieOptions(options));

const redirectToAllowedWebOrigin = (options: Options, candidate: string | null) => {
  const fallback = options.webOrigins[0] ?? DefaultWebOrigin;
  if (!candidate) return fallback;

  try {
    const url = new URL(candidate);
    if (options.webOrigins.includes(url.origin)) return url.toString();
  } catch {
    if (candidate.startsWith("/")) return `${fallback}${candidate}`;
  }

  return fallback;
};

const getReturnTo = (request: HttpServerRequest.HttpServerRequest) => {
  const url = new URL(request.url);
  return url.searchParams.get("returnTo");
};

const makeRuntime = (options: Options): Runtime => ({
  options,
  workos: new WorkOS(Redacted.value(options.apiKey), { clientId: options.clientId }),
});

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const config = yield* ServerConfig.Service;
    const runtime = makeRuntime(config.auth);
    const db = yield* Db.Service;

    return Service.of({
      runtime,
      login: (request) =>
        Effect.sync(() => {
          const url = new URL(request.url);
          const redirectUri = new URL("/auth/callback", url.origin).toString();
          const returnTo = redirectToAllowedWebOrigin(runtime.options, getReturnTo(request));
          const authorizationUrl = runtime.workos.userManagement.getAuthorizationUrl({
            provider: "authkit",
            clientId: runtime.options.clientId,
            redirectUri,
            state: returnTo,
          });

          return HttpServerResponse.redirect(authorizationUrl);
        }).pipe(Effect.orDie),
      callback: (request) =>
        Effect.gen(function* () {
          const url = new URL(request.url);
          const code = url.searchParams.get("code");
          if (!code) {
            return HttpServerResponse.redirect(redirectToAllowedWebOrigin(runtime.options, null));
          }

          const auth = yield* Effect.tryPromise({
            try: () =>
              runtime.workos.userManagement.authenticateWithCode({
                clientId: runtime.options.clientId,
                code,
                session: {
                  sealSession: true,
                  cookiePassword: Redacted.value(runtime.options.cookiePassword),
                },
              }),
            catch: () =>
              new AuthUser.Unauthorized({ message: "Unable to authenticate WorkOS callback" }),
          });

          yield* syncUser(db.client, auth.user);
          const response = HttpServerResponse.redirect(
            redirectToAllowedWebOrigin(runtime.options, url.searchParams.get("state")),
          );

          if (!auth.sealedSession) return response;
          return yield* setSessionCookie(response, auth.sealedSession, runtime.options).pipe(
            Effect.orDie,
          );
        }).pipe(Effect.orDie),
      logout: (request) =>
        Effect.gen(function* () {
          const cookie = request.cookies[SessionCookieName];
          const fallback = redirectToAllowedWebOrigin(runtime.options, getReturnTo(request));

          if (!cookie) {
            return yield* clearSessionCookie(
              HttpServerResponse.redirect(fallback),
              runtime.options,
            ).pipe(Effect.orDie);
          }

          const session = runtime.workos.userManagement.loadSealedSession({
            sessionData: cookie,
            cookiePassword: Redacted.value(runtime.options.cookiePassword),
          });
          const logoutUrl = yield* Effect.tryPromise({
            try: () => session.getLogoutUrl({ returnTo: fallback }),
            catch: () => new AuthUser.Unauthorized({ message: "Unable to build logout URL" }),
          }).pipe(Effect.catch(() => Effect.succeed(fallback)));

          return yield* clearSessionCookie(
            HttpServerResponse.redirect(logoutUrl),
            runtime.options,
          ).pipe(Effect.orDie);
        }).pipe(Effect.orDie),
      authenticateSession: (httpEffect, credential) =>
        Effect.gen(function* () {
          const sessionData = Redacted.value(credential);
          const session = runtime.workos.userManagement.loadSealedSession({
            sessionData,
            cookiePassword: Redacted.value(runtime.options.cookiePassword),
          });

          const authenticateResult = yield* Effect.tryPromise({
            try: () => session.authenticate(),
            catch: () => new AuthUser.Unauthorized({ message: "Missing or invalid session" }),
          });
          if (authenticateResult.authenticated) {
            const user = yield* syncUser(db.client, authenticateResult.user);
            return yield* Effect.provideService(httpEffect, AuthUser.CurrentUser, user);
          }

          const refreshResult = yield* Effect.tryPromise({
            try: () => session.refresh(),
            catch: () => new AuthUser.Unauthorized({ message: "Missing or invalid session" }),
          });

          if (!refreshResult.authenticated) {
            return yield* new AuthUser.Unauthorized({
              message: "Missing or invalid session",
            });
          }

          if (refreshResult.sealedSession) {
            yield* HttpEffect.appendPreResponseHandler((_, response) =>
              setSessionCookie(response, refreshResult.sealedSession!, runtime.options).pipe(
                Effect.orDie,
              ),
            );
          }

          const user = yield* syncUser(db.client, refreshResult.user);
          return yield* Effect.provideService(httpEffect, AuthUser.CurrentUser, user);
        }),
    });
  }),
);

export * as WorkOsAuth from "./WorkOsAuth.ts";
