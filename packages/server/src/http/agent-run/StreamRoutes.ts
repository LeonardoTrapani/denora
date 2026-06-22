import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import { Auth } from "../../auth/Auth.ts";
import { AgentRuns } from "../../agent-run/AgentRuns.ts";

export const routes = HttpRouter.use((router) =>
  router.add("*", "/runs/:runId", streamRoute, { uninterruptible: false }),
);

const streamRoute = (request: HttpServerRequest.HttpServerRequest) =>
  Effect.gen(function* () {
    const params = yield* HttpRouter.params;
    const runId = params.runId;
    if (runId === undefined || runId.length === 0) {
      return HttpServerResponse.fromWeb(Response.json({ error: "RunIdRequired" }, { status: 400 }));
    }

    if (request.method !== "GET" && request.method !== "HEAD") {
      return HttpServerResponse.fromWeb(
        Response.json(
          { error: "MethodNotAllowed" },
          { status: 405, headers: { Allow: "GET, HEAD" } },
        ),
      );
    }

    const auth = yield* Auth.Service;
    const webRequest = yield* HttpServerRequest.toWeb(request);
    const user = yield* auth.requireSession(webRequest).pipe(Effect.result);
    if (Result.isFailure(user)) {
      return HttpServerResponse.fromWeb(Response.json({ error: "Unauthorized" }, { status: 401 }));
    }

    const agentRuns = yield* AgentRuns.Service;
    // TODO(agent-run-auth): add the Agent/Thread/Run registry before this route
    // is considered safe. Flue resolves `/runs/:runId` through its RunStore,
    // verifies the workflow exposes run middleware, then runs that middleware
    // before serving the stream. Denora does not have the equivalent ownership
    // record yet, so this is intentionally only session-gated for the current
    // spike. Reference: ~/.local/share/opencode/repos/github.com/withastro/flue/
    // packages/runtime/src/runtime/flue-app.ts runStreamReadHandler.
    return yield* agentRuns.streamRequest(runId, request);
  });

export * as AgentRunStreamRoutes from "./StreamRoutes.ts";
