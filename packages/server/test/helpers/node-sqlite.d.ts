declare module "node:sqlite" {
  export class DatabaseSync {
    constructor(location: string);
    prepare(query: string): StatementSync;
    close(): void;
  }

  export interface StatementSync {
    all(...values: Array<unknown>): Array<Record<string, unknown>>;
  }
}
