import * as Alchemy from "alchemy";
import * as AdoptPolicy from "alchemy/AdoptPolicy";
import * as Cloudflare from "alchemy/Cloudflare";
import * as Drizzle from "alchemy/Drizzle";
import * as Neon from "alchemy/Neon";
import { AlchemyDb } from "@denora/server/persistence/AlchemyDb";
import { ServerResource } from "@denora/server/server/Resource";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

const RootDomain = "denora.me";

const deployedStages = {
  dev: {
    webDomain: `dev.${RootDomain}`,
    apiDomain: `api.dev.${RootDomain}`,
  },
  staging: {
    webDomain: `staging.${RootDomain}`,
    apiDomain: `api.staging.${RootDomain}`,
  },
  prod: {
    webDomain: RootDomain,
    apiDomain: `api.${RootDomain}`,
  },
} as const;

type Deployment = (typeof deployedStages)[keyof typeof deployedStages];

const deploymentForStage = (stage: string) =>
  Option.fromUndefinedOr((deployedStages as Record<string, Deployment | undefined>)[stage]);

export default Alchemy.Stack(
  "Denora",
  {
    providers: Layer.mergeAll(Cloudflare.providers(), Drizzle.providers()).pipe(
      Layer.provideMerge(Neon.providers()),
    ),
    state: Cloudflare.state(),
  },
  Effect.gen(function* () {
    const stage = yield* Alchemy.Stage;
    const deployment = deploymentForStage(stage);
    const zone = yield* Option.match(deployment, {
      onNone: () => Effect.void,
      onSome: () =>
        Cloudflare.Zone("denora-zone", { name: RootDomain }).pipe(AdoptPolicy.adopt(true)),
    });

    const { branch } = yield* AlchemyDb.DenoraDb;
    const hyperdrive = yield* AlchemyDb.DenoraHyperdrive;

    const server = yield* ServerResource.Resource.pipe(
      Effect.provideService(
        ServerResource.Deployment,
        Option.match(deployment, {
          onNone: () => ({}),
          onSome: ({ apiDomain, webDomain }) => ({ apiDomain, webDomain }),
        }),
      ),
    );

    const serverUrl = server.url.as<string>();

    const webProps = Option.match(deployment, {
      onNone: () => ({
        rootDir: "./packages/web",
        env: {
          VITE_API_URL: serverUrl,
        },
        compatibility: {
          flags: ["nodejs_compat" as const],
        },
      }),
      onSome: ({ webDomain }) => ({
        rootDir: "./packages/web",
        domain: webDomain,
        env: {
          VITE_API_URL: serverUrl,
        },
        compatibility: {
          flags: ["nodejs_compat" as const],
        },
      }),
    });

    const web = yield* Cloudflare.Vite("Web", webProps);
    const domains = Option.getOrUndefined(deployment);

    return {
      apiDomain: domains?.apiDomain,
      databaseName: branch.databaseName,
      hyperdriveId: hyperdrive.hyperdriveId,
      stage,
      serverUrl,
      webDomain: domains?.webDomain,
      webUrl: web.url.as<string>(),
      zoneId: zone?.zoneId,
    };
  }),
);
