import {
  CommandId,
  WorktreeCapabilityUnavailableError,
  WorktreeHandoffAlreadyInWorktreeError,
  type WorktreeHandoffInput,
  WorktreeHandoffInvalidRequestError,
  type WorktreeHandoffResult,
  type WorktreeHandoffSetupScriptStatus,
  WorktreeOperationError,
  WorktreeProjectNotFoundError,
  type WorktreeStatusResult,
  WorktreeThreadNotFoundError,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Path from "effect/Path";

import * as GitWorkflowService from "../../../git/GitWorkflowService.ts";
import * as OrchestrationEngine from "../../../orchestration/Services/OrchestrationEngine.ts";
import * as ProjectionSnapshotQuery from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ProjectSetupScriptRunner from "../../../project/ProjectSetupScriptRunner.ts";
import * as ServerSettings from "../../../serverSettings.ts";
import * as VcsStatusBroadcaster from "../../../vcs/VcsStatusBroadcaster.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { WorktreeToolkit } from "./tools.ts";

type WorktreeOperation = typeof WorktreeOperationError.fields.operation.Type;

const errorDetail = (error: unknown): string => {
  if (error instanceof Error) return error.message;
  if (typeof error === "object" && error !== null && "message" in error) {
    return String((error as { message: unknown }).message);
  }
  return String(error);
};

const asOperationError = (operation: WorktreeOperation) => (error: unknown) =>
  new WorktreeOperationError({ operation, cause: error });

const requireWorktreeCapability = Effect.fn("mcp.requireWorktreeCapability")(function* () {
  const invocation = yield* McpInvocationContext.McpInvocationContext;
  if (!invocation.capabilities.has("worktree")) {
    return yield* new WorktreeCapabilityUnavailableError({
      capability: "worktree",
      environmentId: invocation.environmentId,
      threadId: invocation.threadId,
      providerSessionId: invocation.providerSessionId,
      providerInstanceId: invocation.providerInstanceId,
    });
  }
  return invocation;
});

// Serializes handoffs per thread: two concurrent calls could otherwise both
// pass the worktreePath === null check and each create a worktree, leaving
// one untracked on disk. Module-level state is safe here for the same reason
// it is in McpProviderSession: the server process hosts a single MCP server.
const handoffThreadsInFlight = new Set<string>();

const worktreeHandoff = Effect.fn("WorktreeToolkit.worktreeHandoff")(function* (
  input: WorktreeHandoffInput,
) {
  const invocation = yield* requireWorktreeCapability();
  if (handoffThreadsInFlight.has(invocation.threadId)) {
    return yield* new WorktreeHandoffInvalidRequestError({
      detail: `A worktree handoff is already in progress for thread '${invocation.threadId}'.`,
    });
  }
  handoffThreadsInFlight.add(invocation.threadId);
  return yield* performWorktreeHandoff(invocation, input).pipe(
    Effect.ensuring(Effect.sync(() => handoffThreadsInFlight.delete(invocation.threadId))),
  );
});

const performWorktreeHandoff = Effect.fn("WorktreeToolkit.performWorktreeHandoff")(function* (
  invocation: McpInvocationContext.McpInvocationScope,
  input: WorktreeHandoffInput,
) {
  const crypto = yield* Crypto.Crypto;
  const projectionSnapshotQuery = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const orchestrationEngine = yield* OrchestrationEngine.OrchestrationEngineService;
  const serverSettings = yield* ServerSettings.ServerSettingsService;
  const gitWorkflow = yield* GitWorkflowService.GitWorkflowService;
  const setupScriptRunner = yield* ProjectSetupScriptRunner.ProjectSetupScriptRunner;
  const vcsStatusBroadcaster = yield* VcsStatusBroadcaster.VcsStatusBroadcaster;

  const thread = yield* projectionSnapshotQuery
    .getThreadDetailById(invocation.threadId)
    .pipe(Effect.map(Option.getOrUndefined), Effect.mapError(asOperationError("resolveThread")));
  if (!thread) {
    return yield* new WorktreeThreadNotFoundError({ threadId: invocation.threadId });
  }
  if (thread.worktreePath !== null) {
    return yield* new WorktreeHandoffAlreadyInWorktreeError({
      threadId: invocation.threadId,
      worktreePath: thread.worktreePath,
    });
  }

  const project = yield* projectionSnapshotQuery
    .getProjectShellById(thread.projectId)
    .pipe(Effect.map(Option.getOrUndefined), Effect.mapError(asOperationError("resolveProject")));
  if (!project) {
    return yield* new WorktreeProjectNotFoundError({
      threadId: invocation.threadId,
      projectId: thread.projectId,
    });
  }
  const projectCwd = project.workspaceRoot;

  const path = yield* Path.Path;
  if (input.path !== undefined && !path.isAbsolute(input.path)) {
    return yield* new WorktreeHandoffInvalidRequestError({
      detail: `path must be an absolute filesystem path, got '${input.path}'. A relative path would be created relative to the project workspace but stored verbatim as the thread's worktree binding.`,
    });
  }

  let baseRef = input.baseRef;
  if (baseRef === undefined) {
    const localStatus = yield* gitWorkflow
      .localStatus({ cwd: projectCwd })
      .pipe(Effect.mapError(asOperationError("resolveBaseRef")));
    if (!localStatus.isRepo) {
      return yield* new WorktreeHandoffInvalidRequestError({
        detail: `Project workspace '${projectCwd}' is not a git repository.`,
      });
    }
    if (localStatus.refName === null) {
      return yield* new WorktreeHandoffInvalidRequestError({
        detail:
          "Could not determine the current branch of the project workspace (detached HEAD?). Pass baseRef explicitly.",
      });
    }
    baseRef = localStatus.refName;
  }

  const startFromOrigin =
    input.startFromOrigin ??
    (yield* serverSettings.getSettings.pipe(
      Effect.map((settings) => settings.newWorktreesStartFromOrigin),
      Effect.mapError(asOperationError("resolveSettings")),
    ));

  let worktreeBaseRef = baseRef;
  if (startFromOrigin) {
    yield* gitWorkflow
      .fetchRemote({ cwd: projectCwd, remoteName: "origin" })
      .pipe(Effect.mapError(asOperationError("fetchRemote")));
    const resolvedRemoteBase = yield* gitWorkflow
      .resolveRemoteTrackingCommit({
        cwd: projectCwd,
        refName: baseRef,
        fallbackRemoteName: "origin",
      })
      .pipe(Effect.mapError(asOperationError("resolveRemoteTrackingCommit")));
    worktreeBaseRef = resolvedRemoteBase.commitSha;
  }

  const worktree = yield* gitWorkflow
    .createWorktree({
      cwd: projectCwd,
      refName: worktreeBaseRef,
      newRefName: input.branch,
      baseRefName: baseRef,
      path: input.path ?? null,
    })
    .pipe(Effect.mapError(asOperationError("createWorktree")));
  const worktreePath = worktree.worktree.path;

  const commandId = yield* crypto.randomUUIDv4.pipe(
    Effect.map((uuid) => CommandId.make(`server:mcp-worktree-handoff:${uuid}`)),
    Effect.orDie,
  );
  yield* orchestrationEngine
    .dispatch({
      type: "thread.meta.update",
      commandId,
      threadId: invocation.threadId,
      branch: worktree.worktree.refName,
      worktreePath,
    })
    .pipe(
      Effect.mapError(asOperationError("updateThreadMetadata")),
      // The worktree was already created; if the thread cannot be re-pointed
      // at it, remove it again so a failed handoff leaves nothing behind.
      Effect.catch((error) =>
        gitWorkflow
          .removeWorktree({ cwd: projectCwd, path: worktreePath, force: true })
          .pipe(Effect.ignoreCause({ log: true }), Effect.andThen(Effect.fail(error))),
      ),
    );

  yield* vcsStatusBroadcaster
    .refreshStatus(worktreePath)
    .pipe(Effect.ignoreCause({ log: true }), Effect.forkDetach, Effect.asVoid);

  let setupScript: WorktreeHandoffSetupScriptStatus = { status: "skipped" };
  if (input.runSetupScript ?? true) {
    setupScript = yield* setupScriptRunner
      .runForThread({
        threadId: invocation.threadId,
        projectId: thread.projectId,
        projectCwd,
        worktreePath,
      })
      .pipe(
        Effect.map(
          (result): WorktreeHandoffSetupScriptStatus =>
            result.status === "started"
              ? {
                  status: "started",
                  scriptName: result.scriptName,
                  terminalId: result.terminalId,
                }
              : { status: "no-script" },
        ),
        // catchCause rather than catch: the thread is already re-pointed at the
        // worktree, so even a defect in the setup runner must not fail the handoff.
        Effect.catchCause(
          (cause): Effect.Effect<WorktreeHandoffSetupScriptStatus> =>
            Effect.logWarning("worktree handoff setup script failed", {
              threadId: invocation.threadId,
              worktreePath,
              detail: errorDetail(Cause.squash(cause)),
            }).pipe(
              Effect.as({ status: "failed", detail: errorDetail(Cause.squash(cause)) } as const),
            ),
        ),
      );
  }

  const result: WorktreeHandoffResult = {
    worktreePath,
    branch: worktree.worktree.refName,
    baseRef,
    startedFromOrigin: startFromOrigin,
    setupScript,
    note: "Handoff recorded. The agent session restarts inside the worktree at the start of the next turn with the conversation preserved; finish the current turn normally.",
  };
  return result;
});

const worktreeStatus = Effect.fn("WorktreeToolkit.worktreeStatus")(function* () {
  const invocation = yield* requireWorktreeCapability();
  const projectionSnapshotQuery = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const serverSettings = yield* ServerSettings.ServerSettingsService;

  const thread = yield* projectionSnapshotQuery
    .getThreadDetailById(invocation.threadId)
    .pipe(Effect.map(Option.getOrUndefined), Effect.mapError(asOperationError("resolveThread")));
  if (!thread) {
    return yield* new WorktreeThreadNotFoundError({ threadId: invocation.threadId });
  }

  const project = yield* projectionSnapshotQuery
    .getProjectShellById(thread.projectId)
    .pipe(Effect.map(Option.getOrUndefined), Effect.mapError(asOperationError("resolveProject")));
  if (!project) {
    return yield* new WorktreeProjectNotFoundError({
      threadId: invocation.threadId,
      projectId: thread.projectId,
    });
  }

  const defaultStartFromOrigin = yield* serverSettings.getSettings.pipe(
    Effect.map((settings) => settings.newWorktreesStartFromOrigin),
    Effect.mapError(asOperationError("resolveSettings")),
  );

  const result: WorktreeStatusResult = {
    attached: thread.worktreePath !== null,
    worktreePath: thread.worktreePath,
    branch: thread.branch,
    projectWorkspaceRoot: project.workspaceRoot,
    defaultStartFromOrigin,
  };
  return result;
});

export const WorktreeToolkitHandlersLive = WorktreeToolkit.toLayer({
  worktree_handoff: (input) => worktreeHandoff(input),
  worktree_status: () => worktreeStatus(),
});

/** Exposed for tests. */
export const __testing = {
  worktreeHandoff,
  worktreeStatus,
};
