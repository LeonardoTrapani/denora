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
bun fmt
bun lint
bun check-types
bun run build
```

`bun run dev` runs the full stack through `alchemy dev`. Use `bun run dev:web` only for isolated frontend work.
