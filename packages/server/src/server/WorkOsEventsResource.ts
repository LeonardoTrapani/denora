import * as Cloudflare from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import { WorkOsEvents } from "../auth/WorkOsEvents.ts";
import { WorkOsAuth } from "../auth/WorkOsAuth.ts";
import { ServerConfig } from "../config/ServerConfig.ts";
import { Db } from "../persistence/Db.ts";

export class Resource extends Cloudflare.Worker<Resource>()(
  "WorkOsEventsWorker",
  {
    main: import.meta.filename,
    compatibility: {
      flags: ["nodejs_compat"],
    },
  },
  Effect.gen(function* () {
    const config = yield* ServerConfig.load;

    yield* Cloudflare.cron("*/5 * * * *").subscribe(() =>
      WorkOsEvents.runOnce().pipe(
        Effect.provide([
          WorkOsEvents.layer,
          WorkOsAuth.layer,
          Db.hyperdriveLayer,
          ServerConfig.layer(config),
        ]),
        Effect.catch((error) => Effect.logError("WorkOS event sync failed", error)),
      ),
    );

    return {
      fetch: Effect.succeed(HttpServerResponse.text("OK")),
    };
  }).pipe(Effect.provide(Cloudflare.CronEventSourceLive)),
) {}

export default Resource;

export * as WorkOsEventsResource from "./WorkOsEventsResource.ts";
