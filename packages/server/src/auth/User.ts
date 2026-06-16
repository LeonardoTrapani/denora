import * as Context from "effect/Context";
import * as Schema from "effect/Schema";

/**
 * The authenticated user as exposed to the rest of the app. Mirrors Better
 * Auth's `user` model (dates serialized as ISO strings for the HTTP API).
 */
export class DenoraUser extends Schema.Class<DenoraUser>("DenoraUser")({
  id: Schema.String,
  email: Schema.String,
  emailVerified: Schema.Boolean,
  name: Schema.NullOr(Schema.String),
  image: Schema.NullOr(Schema.String),
  createdAt: Schema.String,
  updatedAt: Schema.String,
}) {}

export class CurrentUser extends Context.Service<CurrentUser, DenoraUser>()(
  "@denora/server/Authorization/CurrentUser",
) {}

export class Unauthorized extends Schema.TaggedErrorClass<Unauthorized>()(
  "Unauthorized",
  { message: Schema.String },
  { httpApiStatus: 401 },
) {}
