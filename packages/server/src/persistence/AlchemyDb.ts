import * as Cloudflare from "alchemy/Cloudflare";
import * as Drizzle from "alchemy/Drizzle";
import * as Neon from "alchemy/Neon";
import * as Effect from "effect/Effect";

const MigrationsDir = "./packages/server/migrations";
const MigrationsTable = "denora_migrations";

export const DenoraDb = Effect.gen(function* () {
  const schema = yield* Drizzle.Schema("denora-schema", {
    schema: "./packages/server/src/persistence/schema.ts",
    out: MigrationsDir,
  });

  const project = yield* Neon.Project("denora-db", {
    region: "aws-us-east-1",
  });

  const branch = yield* Neon.Branch("denora-branch", {
    project,
    migrationsDir: schema.out,
    migrationsTable: MigrationsTable,
  });

  return { project, branch, schema };
});

export const DenoraHyperdrive = Effect.gen(function* () {
  const { branch } = yield* DenoraDb;

  return yield* Cloudflare.Hyperdrive.Connection("denora-hyperdrive", {
    origin: branch.origin,
  });
});

export * as AlchemyDb from "./AlchemyDb.ts";
