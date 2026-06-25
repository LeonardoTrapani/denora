/**
 * Public `@flue/cli/config` subpath. Exposes only what `flue.config.ts`
 * authors need; config discovery and resolution are internal to the CLI.
 */

export { defineConfig, type FlueConfig, type UserFlueConfig } from './lib/config.ts';
