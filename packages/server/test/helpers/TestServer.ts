import { NodeHttpServer } from "@effect/platform-node";
import * as Layer from "effect/Layer";
import * as HttpRouter from "effect/unstable/http/HttpRouter";

// Serves a router/handler layer on an ephemeral port and exposes an
// `HttpClient` bound to it (via `NodeHttpServer.layerTest`), so suites can hit
// the real wire format. Callers provide the app's own dependencies first.
export const layer = <A, E, R>(appLayer: Layer.Layer<A, E, R>) =>
  HttpRouter.serve(appLayer, { disableListenLog: true, disableLogger: true }).pipe(
    Layer.provideMerge(NodeHttpServer.layerTest),
  );
