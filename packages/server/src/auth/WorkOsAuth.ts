import { WorkOS } from "@workos-inc/node";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import { ServerConfig } from "../config/ServerConfig.ts";
import { Db } from "../persistence/Db.ts";
import { UserSync, UserSyncError } from "./UserSync.ts";
import { AuthUser } from "./User.ts";

type Options = ServerConfig.Auth;
type SealedSession = ReturnType<WorkOS["userManagement"]["loadSealedSession"]>;

export const SessionCookieName = "wos-session";

export interface AuthRequestMetadata {
  readonly ipAddress?: string;
  readonly userAgent?: string;
}

export interface AuthenticatedSession {
  readonly user: AuthUser.DenoraUser;
  readonly sealedSession?: string;
}

export interface Interface {
  readonly client: WorkOS;
  readonly getAuthorizationUrl: (input: {
    readonly redirectUri: string;
    readonly returnTo: string;
  }) => Effect.Effect<string, WorkOsAuthError>;
  readonly authenticateWithCode: (input: {
    readonly code: string;
    readonly metadata?: AuthRequestMetadata;
  }) => Effect.Effect<AuthenticatedSession, WorkOsAuthError | UserSyncError>;
  readonly getLogoutUrl: (input: {
    readonly sealedSession: string;
    readonly returnTo: string;
  }) => Effect.Effect<string, WorkOsSessionError>;
  readonly authenticateSession: (
    credential: Redacted.Redacted<string>,
  ) => Effect.Effect<
    AuthenticatedSession,
    AuthUser.Unauthorized | WorkOsSessionError | UserSyncError
  >;
}

export class Service extends Context.Service<Service, Interface>()("@denora/server/WorkOsAuth") {}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const config = yield* ServerConfig.Service;
    const options = config.auth;
    const client = new WorkOS(Redacted.value(options.apiKey), {
      clientId: options.clientId,
    });

    const db = yield* Db.Service;

    return Service.of({
      client,
      getAuthorizationUrl: ({ redirectUri, returnTo }) =>
        getWorkOsAuthorizationUrl(client, options, redirectUri, returnTo),
      authenticateWithCode: ({ code, metadata }) =>
        Effect.gen(function* () {
          const auth = yield* authenticateWorkOsCode(client, options, code, metadata);
          const user = yield* UserSync.syncUser(db.client, auth.user);
          if (!auth.sealedSession) return { user };
          return { user, sealedSession: auth.sealedSession };
        }),
      getLogoutUrl: ({ sealedSession, returnTo }) =>
        Effect.gen(function* () {
          const session = yield* loadSealedSession(client, options, sealedSession);
          return yield* getSealedSessionLogoutUrl(session, returnTo);
        }),
      authenticateSession: (credential) =>
        authenticateCredential(client, db.client, options, credential),
    });
  }),
);

export class WorkOsAuthError extends Schema.TaggedErrorClass<WorkOsAuthError>()("WorkOsAuthError", {
  operation: Schema.String,
  cause: Schema.Defect(),
}) {}

export class WorkOsSessionError extends Schema.TaggedErrorClass<WorkOsSessionError>()(
  "WorkOsSessionError",
  {
    operation: Schema.String,
    cause: Schema.Defect(),
  },
) {}

const getWorkOsAuthorizationUrl = Effect.fn("WorkOsAuth.getWorkOsAuthorizationUrl")(
  (client: WorkOS, options: Options, redirectUri: string, returnTo: string) =>
    Effect.try({
      try: () =>
        client.userManagement.getAuthorizationUrl({
          provider: "authkit",
          clientId: options.clientId,
          redirectUri,
          state: returnTo,
        }),
      catch: (cause) => new WorkOsAuthError({ operation: "getAuthorizationUrl", cause }),
    }),
);

const authenticateWorkOsCode = Effect.fn("WorkOsAuth.authenticateWorkOsCode")(
  (client: WorkOS, options: Options, code: string, metadata: AuthRequestMetadata | undefined) =>
    Effect.tryPromise({
      try: () =>
        client.userManagement.authenticateWithCode({
          clientId: options.clientId,
          code,
          ...metadata,
          session: {
            sealSession: true,
            cookiePassword: Redacted.value(options.cookiePassword),
          },
        }),
      catch: (cause) => new WorkOsAuthError({ operation: "authenticateWithCode", cause }),
    }),
);

const loadSealedSession = Effect.fn("WorkOsAuth.loadSealedSession")(
  (client: WorkOS, options: Options, sessionData: string) =>
    Effect.try({
      try: () =>
        client.userManagement.loadSealedSession({
          sessionData,
          cookiePassword: Redacted.value(options.cookiePassword),
        }),
      catch: (cause) => new WorkOsSessionError({ operation: "loadSealedSession", cause }),
    }),
);

const authenticateSealedSession = Effect.fn("WorkOsAuth.authenticateSealedSession")(
  (session: SealedSession) =>
    Effect.tryPromise({
      try: () => session.authenticate(),
      catch: (cause) => new WorkOsSessionError({ operation: "session.authenticate", cause }),
    }),
);

const refreshSealedSession = Effect.fn("WorkOsAuth.refreshSealedSession")(
  (session: SealedSession) =>
    Effect.tryPromise({
      try: () => session.refresh(),
      catch: (cause) => new WorkOsSessionError({ operation: "session.refresh", cause }),
    }),
);

const getSealedSessionLogoutUrl = Effect.fn("WorkOsAuth.getSealedSessionLogoutUrl")(
  (session: SealedSession, returnTo: string) =>
    Effect.tryPromise({
      try: () => session.getLogoutUrl({ returnTo }),
      catch: (cause) => new WorkOsSessionError({ operation: "session.getLogoutUrl", cause }),
    }),
);

const authenticateCredential = Effect.fn("WorkOsAuth.authenticateCredential")(function* (
  client: WorkOS,
  db: Db.Client,
  options: Options,
  credential: Redacted.Redacted<string>,
) {
  const session = yield* loadSealedSession(client, options, Redacted.value(credential));
  const authenticateResult = yield* authenticateSealedSession(session);

  if (authenticateResult.authenticated) {
    const user = yield* UserSync.syncUser(db, authenticateResult.user);
    return { user };
  }

  const refreshResult = yield* refreshSealedSession(session);
  if (!refreshResult.authenticated) {
    return yield* new AuthUser.Unauthorized({
      message: "Missing or invalid session",
    });
  }

  const user = yield* UserSync.syncUser(db, refreshResult.user);
  if (!refreshResult.sealedSession) return { user };
  return { user, sealedSession: refreshResult.sealedSession };
});

export { UserSyncError } from "./UserSync.ts";

export * as WorkOsAuth from "./WorkOsAuth.ts";
