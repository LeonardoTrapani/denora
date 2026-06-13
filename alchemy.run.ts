import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import * as Drizzle from "alchemy/Drizzle";
import * as Neon from "alchemy/Neon";
import { DenoraDb, DenoraHyperdrive } from "@denora/server/Db";
import Server from "@denora/server/Server";
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
    const { branch } = yield* DenoraDb;
    const hyperdrive = yield* DenoraHyperdrive;
    const server = yield* Server;
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
      webUrl: web.url.as<string>(),
    };
  }),
);
