import { WorkOS } from "@workos-inc/node";
import * as Context from "effect/Context";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";

export interface Options {
  readonly apiKey: Redacted.Redacted<string>;
  readonly clientId: string;
  readonly cookiePassword: Redacted.Redacted<string>;
  readonly cookieDomain: string | undefined;
  readonly webOrigins: readonly [string, ...Array<string>];
}

export interface Runtime {
  readonly options: Options;
  readonly workos: WorkOS;
}

export interface Interface {
  readonly runtime: Runtime;
}

export class Service extends Context.Service<Service, Interface>()("@denora/server/WorkOsAuth") {}

export const layer = (options: Options): Layer.Layer<Service> =>
  Layer.succeed(
    Service,
    Service.of({
      runtime: {
        options,
        workos: new WorkOS(Redacted.value(options.apiKey), { clientId: options.clientId }),
      },
    }),
  );

export * as WorkOsAuth from "./WorkOsAuth.ts";
