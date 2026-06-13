import * as Schema from "effect/Schema";

export class Health extends Schema.Class<Health>("Health")({
  status: Schema.Literal("ok"),
}) {}
