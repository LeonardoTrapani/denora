This project is currently in an early, high-velocity development phase. Nothing is in production, and substantial changes are both expected and encouraged.

Agents should prioritize improving the overall design and quality of the system over preserving existing implementations. Breaking changes are acceptable and often desirable if they lead to a better architecture or developer experience.

## Task Completion Requirements

- Use `bun`; do not switch package managers unless explicitly asked.
- Run the relevant available checks before considering tasks completed. In the current skeleton this usually means `bun fmt`, `bun lint`, `bun check-types`
- When a test script exists, use `bun run test`. NEVER run `bun test`. You are not required to run all the tests if it's not needed to the task.

## Denora

`denora` is a Bun Turborepo monorepo for a secure personal agent product with explicit agent identity, understandable controls, permissions, approvals, and a chat-first mobile/web interface.

- Use `bun`; root workspaces `packages/*`.
- No app framework or package layout has been selected yet. Do not create apps or packages without asking when the stack is unclear.
- Product context lives in `CONTEXT.md`. Read it before making product, UX, naming, or architecture decisions.

- Vendored upstream repositories live under `vendor/` as squashed git subtrees. The active vendored references are `vendor/effect-smol`, `vendor/alchemy-effect`, and `vendor/flue`.
- Treat `vendor/*` as read-only reference material: inspect it for source, tests, module structure, and idioms; do not edit it unless explicitly asked; do not import from it in application code.
- Every time you write Effect code, first inspect `vendor/effect-smol/LLMS.md`, then check `vendor/effect-smol/ai-docs` and relevant source/tests under `vendor/effect-smol/packages/*` for idiomatic Effect v4 patterns.

# Module Shape

Do not use `export namespace Foo { ... }` for module organization. It is not standard ESM, it prevents tree-shaking, and it breaks Node's native TypeScript runner.

Use flat top-level exports combined with a self-reexport at the bottom of the file:

```ts
// src/foo/Foo.ts
export interface Interface { ... }
export class Service extends Context.Service<Service, Interface>()("@denora/Foo") {}
export const layer = Layer.effect(Service, ...)
export const defaultLayer = layer.pipe(...)

export * as Foo from "./Foo.ts"
```

Consumers import the namespace projection:

```ts
import { Foo } from "./foo/Foo.ts";

yield * Foo.Service;
Foo.layer;
Foo.defaultLayer;
```

Namespace-private helpers stay as non-exported top-level declarations in the same file. They remain inaccessible to consumers through `export * as`, but are usable by the file's own code.

When the module is an `index.ts` single-namespace directory, use `"."` for the self-reexport source rather than `"./index.ts"`:

```ts
// src/foo/index.ts
export const thing = ...

export * as Foo from "."
```

For directories with several independent modules, keep each sibling as its own file with its own self-reexport, and avoid adding a barrel `index.ts`. Consumers should import the specific sibling:

```ts
import { SessionRetry } from "./session/retry.ts";
import { SessionStatus } from "./session/status.ts";
```

Barrels in multi-sibling directories force every import through the barrel to evaluate every sibling, which defeats tree-shaking and slows module load.
