import type { User as WorkOsUser } from "@workos-inc/node";
import { eq } from "drizzle-orm";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { Db } from "../persistence/Db.ts";
import { schema } from "../persistence/schema.ts";
import { DenoraUser } from "./User.ts";

declare const crypto: { randomUUID(): string };

type UserRow = typeof schema.users.$inferSelect;

export class UserSyncError extends Schema.TaggedErrorClass<UserSyncError>()("UserSyncError", {
  workosUserId: Schema.String,
  cause: Schema.Defect(),
}) {}

export const syncUser = Effect.fn("UserSync.syncUser")(function* (
  client: Db.Client,
  workosUser: WorkOsUser,
) {
  const existing = yield* getExistingUser(client, workosUser.id);
  if (existing !== null && isStaleActiveProjection(existing, workosUser)) {
    return toDenoraUser(existing);
  }

  return yield* upsertUser(client, workosUser, {
    deletedAt: null,
    workosDeletedAt: null,
  });
});

export const syncDeletedUser = Effect.fn("UserSync.syncDeletedUser")(function* (
  client: Db.Client,
  workosUser: WorkOsUser,
  deletedAt: string,
) {
  const existing = yield* getExistingUser(client, workosUser.id);
  if (existing !== null && isStaleDeletedProjection(existing, workosUser, deletedAt)) {
    return toDenoraUser(existing);
  }

  return yield* upsertUser(client, workosUser, {
    deletedAt,
    workosDeletedAt: deletedAt,
  });
});

const getExistingUser = Effect.fn("UserSync.getExistingUser")(function* (
  client: Db.Client,
  workosUserId: string,
) {
  const rows = yield* client
    .select()
    .from(schema.users)
    .where(eq(schema.users.workosUserId, workosUserId))
    .limit(1)
    .pipe(Effect.mapError((cause) => new UserSyncError({ workosUserId, cause })));

  return rows[0] ?? null;
});

const upsertUser = Effect.fn("UserSync.upsertUser")(function* (
  client: Db.Client,
  workosUser: WorkOsUser,
  deletionState: {
    readonly deletedAt: string | null;
    readonly workosDeletedAt: string | null;
  },
) {
  const now = new Date().toISOString();
  const values = {
    email: workosUser.email,
    emailVerified: workosUser.emailVerified,
    name: workosUser.name,
    firstName: workosUser.firstName,
    lastName: workosUser.lastName,
    profilePictureUrl: workosUser.profilePictureUrl,
    locale: workosUser.locale,
    lastSignInAt: workosUser.lastSignInAt,
    workosCreatedAt: workosUser.createdAt,
    workosUpdatedAt: workosUser.updatedAt,
    deletedAt: deletionState.deletedAt,
    workosDeletedAt: deletionState.workosDeletedAt,
    updatedAt: now,
  };

  const rows = yield* client
    .insert(schema.users)
    .values({
      id: crypto.randomUUID(),
      workosUserId: workosUser.id,
      ...values,
      createdAt: now,
    })
    .onConflictDoUpdate({
      target: schema.users.workosUserId,
      set: values,
    })
    .returning()
    .pipe(Effect.mapError((cause) => new UserSyncError({ workosUserId: workosUser.id, cause })));

  const row = rows[0];
  if (!row) {
    return yield* new UserSyncError({
      workosUserId: workosUser.id,
      cause: new Error("User upsert did not return a row"),
    });
  }

  return toDenoraUser(row);
});

export const isStaleActiveProjection = (existing: UserRow, workosUser: WorkOsUser) =>
  existing.workosUpdatedAt > workosUser.updatedAt ||
  (existing.workosDeletedAt !== null && existing.workosDeletedAt >= workosUser.updatedAt);

export const isStaleDeletedProjection = (
  existing: UserRow,
  workosUser: WorkOsUser,
  deletedAt: string,
) =>
  existing.workosUpdatedAt > workosUser.updatedAt ||
  (existing.workosDeletedAt !== null && existing.workosDeletedAt >= deletedAt);

const toDenoraUser = (row: UserRow) =>
  new DenoraUser({
    id: row.id,
    workosUserId: row.workosUserId,
    email: row.email,
    emailVerified: row.emailVerified,
    name: row.name,
    firstName: row.firstName,
    lastName: row.lastName,
    profilePictureUrl: row.profilePictureUrl,
    locale: row.locale,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
  });

export * as UserSync from "./UserSync.ts";
