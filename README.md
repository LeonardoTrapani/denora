# Denora

Bun + Turborepo monorepo for Denora, deployed and developed as a single Alchemy stack.

## Structure

- `packages/*` for shared packages when they are needed.
- `alchemy.run.ts` defines the full-stack Alchemy stack.
- `CONTEXT.md` captures the product direction.
- `AGENTS.md` captures repository guidance for agents.
- `vendor/` contains read-only upstream reference subtrees (`effect-smol`, `alchemy-effect`, and `flue`).

## Commands

```sh
bun install
bun run dev
bun run deploy:dev
bun run deploy:staging
bun run deploy:prod
bun fmt
bun lint
bun check-types
bun run build
```

`bun run dev` runs the full stack through `alchemy dev --stage local` and keeps the app on localhost. Use `bun run dev:web` only for isolated frontend work.

## Stages And Domains

Alchemy stages isolate deployed resources. Local development is the `local` stage and uses localhost. Deployed stages use Cloudflare custom domains managed from `alchemy.run.ts`:

- `dev`: `dev.denora.me`, `api.dev.denora.me`
- `staging`: `staging.denora.me`, `api.staging.denora.me`
- `prod`: `denora.me`, `api.denora.me`

The stack adopts the existing `denora.me` Cloudflare zone and attaches Workers to the stage domains. Worker/public URL values are derived from the stage map, not from `.env`.

## Environment

`.env*` files contain only secrets and per-stage WorkOS client credentials. Public URLs and CORS origins are derived by Alchemy from the stage domains.

Use one WorkOS client per stage with these Redirect URIs. Local development pins the web worker to `localhost:1337` and the API worker to `localhost:1338`:

- local: `http://localhost:1338/api/auth/callback`
- dev: `https://api.dev.denora.me/api/auth/callback`
- staging: `https://api.staging.denora.me/api/auth/callback`
- prod: `https://api.denora.me/api/auth/callback`

```sh
cp .env.example .env
cp .env.dev.example .env.dev
cp .env.staging.example .env.staging
cp .env.production.example .env.production
bun run deploy:dev
bun run deploy:staging
bun run deploy:prod
```

Runtime server secrets such as `WORKOS_API_KEY`, `WORKOS_CLIENT_ID`, and `WORKOS_COOKIE_PASSWORD` stay in the stage env file as deploy-time inputs. Alchemy reads them through `effect/Config` during Worker init and binds them to Cloudflare as encrypted Worker secrets.
