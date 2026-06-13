import type { User as WorkOsUser } from "@workos-inc/node";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import * as HttpEffect from "effect/unstable/http/HttpEffect";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import * as HttpApiMiddleware from "effect/unstable/httpapi/HttpApiMiddleware";
import * as HttpApiSecurity from "effect/unstable/httpapi/HttpApiSecurity";
import { WorkOsAuth } from "./WorkOsAuth.ts";
import { schema } from "../persistence/schema.ts";

declare const crypto: { randomUUID(): string };

type UserRow = typeof schema.users.$inferSelect;
export type DbClient = any;

export const SessionCookieName = "wos-session";

export class DenoraUser extends Schema.Class<DenoraUser>("DenoraUser")({
  id: Schema.String,
  workosUserId: Schema.String,
  email: Schema.String,
  emailVerified: Schema.Boolean,
  name: Schema.NullOr(Schema.String),
  firstName: Schema.NullOr(Schema.String),
  lastName: Schema.NullOr(Schema.String),
  profilePictureUrl: Schema.NullOr(Schema.String),
  locale: Schema.NullOr(Schema.String),
  createdAt: Schema.String,
  updatedAt: Schema.String,
}) {}

export class CurrentUser extends Context.Service<CurrentUser, DenoraUser>()(
  "@denora/server/Authorization/CurrentUser",
) {}

export class Unauthorized extends Schema.TaggedErrorClass<Unauthorized>()(
  "Unauthorized",
  { message: Schema.String },
  { httpApiStatus: 401 },
) {}

export const sessionCookie = HttpApiSecurity.apiKey({
  in: "cookie",
  key: SessionCookieName,
});

export class Service extends HttpApiMiddleware.Service<
  Service,
  {
    provides: CurrentUser;
    requires: never;
  }
>()("@denora/server/Authorization", {
  security: {
    session: sessionCookie,
  },
  error: Unauthorized,
}) {}

const cookieOptions = (options: WorkOsAuth.Options) => ({
  domain: options.cookieDomain,
  path: "/",
  httpOnly: true,
  secure: true,
  sameSite: "lax" as const,
});

const toDenoraUser = (row: UserRow) =>
  new DenoraUser({
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

const syncUser = (db: DbClient, workosUser: WorkOsUser) =>
  Effect.gen(function* () {
    const now = new Date().toISOString();
    const rows = yield* db
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
      .pipe(Effect.orDie) as Effect.Effect<ReadonlyArray<UserRow>>;

    return toDenoraUser(rows[0]!);
  });

const setSessionCookie = (
  response: HttpServerResponse.HttpServerResponse,
  value: string,
  options: WorkOsAuth.Options,
) => HttpServerResponse.setCookie(response, SessionCookieName, value, cookieOptions(options));

const clearSessionCookie = (
  response: HttpServerResponse.HttpServerResponse,
  options: WorkOsAuth.Options,
) => HttpServerResponse.expireCookie(response, SessionCookieName, cookieOptions(options));

const redirectToAllowedWebOrigin = (options: WorkOsAuth.Options, candidate: string | null) => {
  const fallback = options.webOrigins[0];
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

export const layer = (db: DbClient): Layer.Layer<Service, never, WorkOsAuth.Service> =>
  Layer.effect(
    Service,
    Effect.gen(function* () {
      const auth = yield* WorkOsAuth.Service;

      return Service.of({
        session: (httpEffect, { credential }) =>
          Effect.gen(function* () {
            const sessionData = Redacted.value(credential);
            const session = auth.runtime.workos.userManagement.loadSealedSession({
              sessionData,
              cookiePassword: Redacted.value(auth.runtime.options.cookiePassword),
            });

            const authenticateResult = yield* Effect.tryPromise({
              try: () => session.authenticate(),
              catch: () => new Unauthorized({ message: "Missing or invalid session" }),
            });
            if (authenticateResult.authenticated) {
              const user = yield* syncUser(db, authenticateResult.user);
              return yield* Effect.provideService(httpEffect, CurrentUser, user);
            }

            const refreshResult = yield* Effect.tryPromise({
              try: () => session.refresh(),
              catch: () => new Unauthorized({ message: "Missing or invalid session" }),
            });

            if (!refreshResult.authenticated) {
              return yield* new Unauthorized({
                message: "Missing or invalid session",
              });
            }

            if (refreshResult.sealedSession) {
              yield* HttpEffect.appendPreResponseHandler((_, response) =>
                setSessionCookie(response, refreshResult.sealedSession!, auth.runtime.options).pipe(
                  Effect.orDie,
                ),
              );
            }

            const user = yield* syncUser(db, refreshResult.user);
            return yield* Effect.provideService(httpEffect, CurrentUser, user);
          }),
      });
    }),
  );

const login = (runtime: WorkOsAuth.Runtime) => (request: HttpServerRequest.HttpServerRequest) =>
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
  }).pipe(Effect.orDie);

const callback =
  (db: DbClient, runtime: WorkOsAuth.Runtime) => (request: HttpServerRequest.HttpServerRequest) =>
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
        catch: () => new Unauthorized({ message: "Unable to authenticate WorkOS callback" }),
      });

      yield* syncUser(db, auth.user);
      const response = HttpServerResponse.redirect(
        redirectToAllowedWebOrigin(runtime.options, url.searchParams.get("state")),
      );

      if (!auth.sealedSession) return response;
      return yield* setSessionCookie(response, auth.sealedSession, runtime.options).pipe(
        Effect.orDie,
      );
    }).pipe(Effect.orDie);

const logout = (runtime: WorkOsAuth.Runtime) => (request: HttpServerRequest.HttpServerRequest) =>
  Effect.gen(function* () {
    const cookie = request.cookies[SessionCookieName];
    const fallback = redirectToAllowedWebOrigin(runtime.options, getReturnTo(request));

    if (!cookie) {
      return yield* clearSessionCookie(HttpServerResponse.redirect(fallback), runtime.options).pipe(
        Effect.orDie,
      );
    }

    const session = runtime.workos.userManagement.loadSealedSession({
      sessionData: cookie,
      cookiePassword: Redacted.value(runtime.options.cookiePassword),
    });
    const logoutUrl = yield* Effect.tryPromise({
      try: () => session.getLogoutUrl({ returnTo: fallback }),
      catch: () => new Unauthorized({ message: "Unable to build logout URL" }),
    }).pipe(Effect.catch(() => Effect.succeed(fallback)));

    return yield* clearSessionCookie(HttpServerResponse.redirect(logoutUrl), runtime.options).pipe(
      Effect.orDie,
    );
  }).pipe(Effect.orDie);

export const routes = (db: DbClient) =>
  HttpRouter.use((router) =>
    Effect.gen(function* () {
      const auth = yield* WorkOsAuth.Service;

      yield* router.add("GET", "/auth/login", login(auth.runtime));
      yield* router.add("GET", "/auth/callback", callback(db, auth.runtime));
      yield* router.add("POST", "/auth/logout", logout(auth.runtime));
    }),
  );

export * as Authorization from "./Authorization.ts";
