# Denora

Bun + Turborepo monorepo for Denora, deployed and developed as a single Alchemy stack.

## Structure

- `packages/*` for shared packages when they are needed.
- `alchemy.run.ts` defines the full-stack Alchemy stack.
- `CONTEXT.md` captures the product direction.
- `AGENTS.md` captures repository guidance for agents.
- `opencode.json` configures the `effect-v4` reference repo.

## Commands

```sh
bun install
bun run dev
bun run dev:mobile
bun run deploy:staging
bun fmt
bun lint
bun check-types
bun run build
```

`bun run dev` runs the full stack through `alchemy dev`. Use `bun run dev:web` only for isolated frontend work.
Use `.env.mobile` to point the Expo app at an Alchemy-deployed or locally served API.

## Staging

Alchemy stages isolate deployed resources. Profiles only select which local cloud credentials Alchemy uses, so this repo uses the default profile and targets staging with `--stage staging`.

```sh
cp .env.staging.example .env.staging
cp .env.mobile.example .env.mobile
bun run deploy:staging
```

After deploy, copy the printed `mobileApiUrl` into `.env.mobile` and run:

```sh
bun run dev:mobile
```

Runtime server secrets such as `WORKOS_API_KEY` and `WORKOS_COOKIE_PASSWORD` stay in `.env.staging` as deploy-time inputs. Alchemy reads them through `effect/Config` during Worker init and binds them to Cloudflare as encrypted Worker secrets. `.env.mobile` is intentionally separate because it contains public mobile config, not secrets.
