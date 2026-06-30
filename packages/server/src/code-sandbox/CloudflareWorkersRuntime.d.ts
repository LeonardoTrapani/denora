declare module "cloudflare:workers" {
  export class RpcTarget {
    constructor();
  }

  export class WorkerEntrypoint<Env = unknown> {
    readonly env: Env;
    constructor(ctx?: unknown, env?: Env);
  }

  export const env: Record<string, unknown>;
}
