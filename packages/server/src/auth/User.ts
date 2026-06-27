import * as Context from "effect/Context";
import * as Schema from "effect/Schema";

/**
 * The authenticated user as exposed to the rest of the app. Mirrors the stable
 * user shape Denora needs from WorkOS AuthKit.
 */
export const DenoraUser = Schema.Struct({
  id: Schema.String,
  email: Schema.String,
  emailVerified: Schema.Boolean,
  name: Schema.NullOr(Schema.String),
  image: Schema.NullOr(Schema.String),
  createdAt: Schema.String,
  updatedAt: Schema.String,
}).pipe(Schema.annotate({ identifier: "DenoraUser" }));
export type DenoraUser = typeof DenoraUser.Type;

export class CurrentUser extends Context.Service<CurrentUser, DenoraUser>()(
  "@denora/server/Authorization/CurrentUser",
) {}

export class Unauthorized extends Schema.TaggedErrorClass<Unauthorized>()(
  "Unauthorized",
  { message: Schema.String },
  { httpApiStatus: 401 },
) {}
