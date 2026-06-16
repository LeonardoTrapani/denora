// better-auth's generator emits a little more than this project needs:
// - legacy drizzle `relations()`, removed in drizzle-orm v1
// - `account.password`, only needed for email/password auth
//
// Runtime auth is Google-only and the custom adapter issues plain queries, so
// strip those artifacts after generation to keep the schema importable and lean.
import { readFileSync, writeFileSync } from "node:fs";

const file = "src/persistence/schema/auth.ts";

let source = readFileSync(file, "utf8");
source = source.replace(/^import \{ relations \} from "drizzle-orm";\n/m, "");
source = source.replace(/^\s{4}password: text\("password"\),\n/m, "");

const relationsStart = source.search(/^export const \w+ = relations\(/m);
if (relationsStart !== -1) {
  source = `${source.slice(0, relationsStart).trimEnd()}\n`;
}

writeFileSync(file, source);
console.log(`Stripped unused Better Auth schema artifacts from ${file}`);
