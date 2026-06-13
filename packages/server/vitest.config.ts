import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
    setupFiles: ["./vitest.setup.ts"],
    // DB suites stand up a Postgres container; HTTP suites bind an ephemeral
    // port. Give per-test work room while keeping the default tight enough to
    // catch hangs.
    testTimeout: 30_000,
    hookTimeout: 120_000,
  },
});
