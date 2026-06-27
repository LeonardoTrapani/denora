import * as Schema from "effect/Schema";

export const Health = Schema.Struct({
  status: Schema.Literal("ok"),
}).pipe(Schema.annotate({ identifier: "Health" }));
export type Health = typeof Health.Type;
