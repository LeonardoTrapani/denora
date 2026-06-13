This project is currently in an early, high-velocity development phase. Nothing is in production, and substantial changes are both expected and encouraged.

Agents should prioritize improving the overall design and quality of the system over preserving existing implementations. Breaking changes are acceptable and often desirable if they lead to a better architecture or developer experience.

## Task Completion Requirements

- Use `bun`; do not switch package managers unless explicitly asked.
- Run the relevant available checks before considering tasks completed. In the current skeleton this usually means `bun fmt`, `bun lint`, `bun check-types`, and `bun run build`.
- When a test script exists, use `bun run test`. NEVER run `bun test`. You are not required to run all the tests if it's not needed to the task.

## Daenya

`daenya` is a Bun Turborepo monorepo for a secure personal agent product with explicit agent identity, understandable controls, permissions, approvals, and a chat-first mobile/web interface.

- Use `bun`; root workspaces `packages/*`.
- No app framework or package layout has been selected yet. Do not create apps or packages without asking when the stack is unclear.
- Product context lives in `CONTEXT.md`. Read it before making product, UX, naming, or architecture decisions.

- Every time you write Effect code, first inspect the `effect-v4` opencode reference. Start with `LLMS.md`, then check `ai-docs` and relevant source/tests under `packages/*` for idiomatic Effect v4 patterns.
- Before writing or reshaping services, layers, module boundaries, read `docs/module-shape.md` and follow its module organization rules.
