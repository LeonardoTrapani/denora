import * as Context from "effect/Context";
import * as Schema from "effect/Schema";

export class DenoraUser extends Schema.Class<DenoraUser>("DenoraUser")({
  id: Schema.String,
  workosUserId: Schema.String,
  email: Schema.String,
  emailVerified: Schema.Boolean,
  name: Schema.NullOr(Schema.String),
  firstName: Schema.NullOr(Schema.String),
  lastName: Schema.NullOr(Schema.String),
  profilePictureUrl: Schema.NullOr(Schema.String),
  locale: Schema.NullOr(Schema.String),
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

export * as AuthUser from "./User.ts";
