import { addEqualityTesters } from "@effect/vitest";

// Teaches vitest's `deepStrictEqual` to use Effect's `Equal` instances, so
// assertions on Schema classes / Data values compare by value not reference.
addEqualityTesters();
