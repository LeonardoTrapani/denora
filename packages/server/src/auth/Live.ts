import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import { ServerConfig } from "../config/ServerConfig.ts";
import { Db } from "../persistence/Db.ts";
import { account, session, user, verification } from "../persistence/schema/auth.ts";
import { Auth, AuthProviderError } from "./Auth.ts";
import { makeAuthDbAdapter, type AuthTableSchema } from "./AuthDbAdapter.ts";
import { AuthRequestContext } from "./AuthRequestContext.ts";
import { makeBetterAuth, type BetterAuthRuntimeOptions } from "./BetterAuth.ts";
import { DenoraUser, Unauthorized } from "./User.ts";

type AuthOptions = Omit<BetterAuthRuntimeOptions, "database">;

const authTables = { user, session, account, verification } as unknown as AuthTableSchema;

type SessionUser = {
  readonly id: string;
  readonly email: string;
  readonly emailVerified: boolean;
  readonly name?: string | null | undefined;
  readonly image?: string | null | undefined;
  readonly createdAt: Date | string;
  readonly updatedAt: Date | string;
};

const toDenoraUser = (user: SessionUser): DenoraUser =>
  new DenoraUser({
    id: user.id,
    email: user.email,
    emailVerified: user.emailVerified,
    name: user.name ?? null,
    image: user.image ?? null,
    createdAt: new Date(user.createdAt).toISOString(),
    updatedAt: new Date(user.updatedAt).toISOString(),
  });

export const layer = (options: AuthOptions): Layer.Layer<Auth.Service, never, Db.Service> =>
  Layer.effect(
    Auth.Service,
    Effect.gen(function* () {
      const db = yield* Db.Service;
      const raw = makeBetterAuth({
        ...options,
        database: makeAuthDbAdapter(db.client, authTables),
      });

      // Capture the per-request Effect context so the Promise-based adapter can
      // run its queries on the request's pooled connection. It carries alchemy's
      // ExecutionContext at runtime (WorkerBridge provides it for every request)
      // even though it is type-erased to `never`, matching how alchemy types its
      // own proxy queries — so no cast is needed.
      const captureContext = Effect.context<never>();

      const handle = Effect.fn("Auth.handle")(function* (request: Request) {
        const context = yield* captureContext;
        return yield* Effect.tryPromise({
          try: () => AuthRequestContext.runWith(context, () => raw.handler(request)),
          catch: (cause) => new AuthProviderError({ operation: "handle", cause }),
        });
      });

      const getSession = Effect.fn("Auth.getSession")(function* (request: Request) {
        const context = yield* captureContext;
        return yield* Effect.tryPromise({
          try: () =>
            AuthRequestContext.runWith(context, async () => {
              const result = await raw.api.getSession({ headers: request.headers });
              return result === null
                ? Option.none<DenoraUser>()
                : Option.some(toDenoraUser(result.user));
            }),
          catch: (cause) => new AuthProviderError({ operation: "getSession", cause }),
        });
      });

      const requireSession = Effect.fn("Auth.requireSession")(function* (request: Request) {
        const session = yield* getSession(request);
        if (Option.isNone(session)) {
          return yield* new Unauthorized({ message: "Authentication required" });
        }
        return session.value;
      });

      return Auth.Service.of({ handle, getSession, requireSession });
    }),
  );

export const layerFromConfig: Layer.Layer<Auth.Service, never, Db.Service | ServerConfig.Service> =
  Layer.unwrap(
    Effect.gen(function* () {
      const config = yield* ServerConfig.Service;
      return layer({
        secret: config.auth.secret,
        baseURL: config.auth.baseURL,
        trustedOrigins: config.auth.webOrigins,
        google: config.auth.google,
      });
    }),
  );

export * as AuthLive from "./Live.ts";
