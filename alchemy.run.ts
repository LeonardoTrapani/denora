import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import * as Drizzle from "alchemy/Drizzle";
import * as Neon from "alchemy/Neon";
import { AlchemyDb } from "@denora/server/persistence/AlchemyDb";
import { ServerResource } from "@denora/server/server/Resource";
import { WorkOsEventsResource } from "@denora/server/server/WorkOsEventsResource";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

export default Alchemy.Stack(
  "Denora",
  {
    providers: Layer.mergeAll(Cloudflare.providers(), Drizzle.providers()).pipe(
      Layer.provideMerge(Neon.providers()),
    ),
    state: Cloudflare.state(),
  },
  Effect.gen(function* () {
    const { branch } = yield* AlchemyDb.DenoraDb;
    const hyperdrive = yield* AlchemyDb.DenoraHyperdrive;
    const server = yield* ServerResource.Resource;
    const workosEvents = yield* WorkOsEventsResource.Resource;
    const serverUrl = server.url.as<string>();

    const web = yield* Cloudflare.Vite("Web", {
      rootDir: "./packages/web",
      env: {
        VITE_API_URL: serverUrl,
      },
      compatibility: {
        flags: ["nodejs_compat"],
      },
    });

    return {
      databaseName: branch.databaseName,
      hyperdriveId: hyperdrive.hyperdriveId,
      serverUrl,
      mobileApiUrl: serverUrl,
      workosEventsWorkerName: workosEvents.workerName,
      webUrl: web.url.as<string>(),
    };
  }),
);
