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
