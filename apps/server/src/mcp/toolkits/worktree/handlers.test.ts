import { describe, expect, it, vi } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  EnvironmentId,
  type OrchestrationProjectShell,
  type OrchestrationThread,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import * as GitWorkflowService from "../../../git/GitWorkflowService.ts";
import * as OrchestrationEngine from "../../../orchestration/Services/OrchestrationEngine.ts";
import * as ProjectionSnapshotQuery from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ProjectSetupScriptRunner from "../../../project/ProjectSetupScriptRunner.ts";
import * as ServerSettings from "../../../serverSettings.ts";
import { VcsStatusBroadcaster } from "../../../vcs/VcsStatusBroadcaster.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { __testing } from "./handlers.ts";

const environmentId = EnvironmentId.make("environment-worktree-test");
const threadId = ThreadId.make("thread-worktree-test");
const projectId = ProjectId.make("project-worktree-test");
const workspaceRoot = "/repo/project";

const makeInvocationLayer = (capabilities: ReadonlySet<McpInvocationContext.McpCapability>) =>
  Layer.succeed(McpInvocationContext.McpInvocationContext, {
    environmentId,
    threadId,
    providerSessionId: "provider-session-worktree-test",
    providerInstanceId: ProviderInstanceId.make("claudeAgent"),
    capabilities,
    issuedAt: 1,
    expiresAt: Number.MAX_SAFE_INTEGER,
  });

const makeThread = (overrides: Partial<OrchestrationThread> = {}): OrchestrationThread =>
  ({
    id: threadId,
    projectId,
    title: "Worktree test thread",
    modelSelection: {
      instanceId: ProviderInstanceId.make("claudeAgent"),
      model: "claude-sonnet-5",
    },
    runtimeMode: "approval-required",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    latestTurn: null,
    createdAt: "2026-01-01T00:00:00.000Z",
    updatedAt: "2026-01-01T00:00:00.000Z",
    archivedAt: null,
    deletedAt: null,
    messages: [],
    proposedPlans: [],
    activities: [],
    checkpoints: [],
    session: null,
    ...overrides,
  }) as OrchestrationThread;

const projectShell: OrchestrationProjectShell = {
  id: projectId,
  title: "Worktree test project",
  workspaceRoot,
  defaultModelSelection: null,
  scripts: [],
  createdAt: "2026-01-01T00:00:00.000Z",
  updatedAt: "2026-01-01T00:00:00.000Z",
  deletedAt: null,
} as OrchestrationProjectShell;

interface HarnessOptions {
  readonly thread?: OrchestrationThread | null;
  readonly capabilities?: ReadonlySet<McpInvocationContext.McpCapability>;
  readonly currentBranch?: string | null;
  readonly newWorktreesStartFromOrigin?: boolean;
  readonly setupScript?: "started" | "no-script" | "fails" | "dies";
  readonly dispatchFails?: boolean;
  readonly createWorktreeGate?: Effect.Effect<void>;
}

const makeHarness = (options: HarnessOptions = {}) => {
  const thread = options.thread === undefined ? makeThread() : options.thread;
  const dispatch = vi.fn((_: unknown) =>
    options.dispatchFails
      ? (Effect.fail("simulated dispatch failure") as never)
      : Effect.succeed({ sequence: 1 }),
  );
  const removeWorktree = vi.fn((_: unknown) => Effect.void);
  const fetchRemote = vi.fn((_: unknown) => Effect.void);
  const resolveRemoteTrackingCommit = vi.fn((_: unknown) =>
    Effect.succeed({ commitSha: "abc123", remoteRefName: "origin/dev" }),
  );
  const createWorktree = vi.fn(
    (input: { readonly newRefName?: string | undefined; readonly path: string | null }) =>
      (options.createWorktreeGate ?? Effect.void).pipe(
        Effect.andThen(
          Effect.succeed({
            worktree: {
              path: input.path ?? `/worktrees/project/${input.newRefName}`,
              refName: input.newRefName ?? "detached",
            },
          }),
        ),
      ),
  );
  const localStatus = vi.fn((_: unknown) =>
    Effect.succeed({
      isRepo: true,
      hasPrimaryRemote: true,
      isDefaultRef: false,
      refName: options.currentBranch === undefined ? "dev" : options.currentBranch,
      hasWorkingTreeChanges: false,
      workingTree: { files: [], insertions: 0, deletions: 0 },
    }),
  );
  const refreshStatus = vi.fn((_: string) => Effect.die("refreshStatus stub"));
  const runForThread = vi.fn((input: { readonly worktreePath: string }) => {
    switch (options.setupScript ?? "started") {
      case "no-script":
        return Effect.succeed({ status: "no-script" } as const);
      case "dies":
        return Effect.die(new Error("setup runner defect"));
      case "fails":
        return Effect.fail(
          new ProjectSetupScriptRunner.ProjectSetupScriptProjectNotFoundError({
            threadId,
            worktreePath: input.worktreePath,
          }),
        );
      default:
        return Effect.succeed({
          status: "started",
          scriptId: "setup",
          scriptName: "Setup",
          terminalId: "setup-terminal",
          cwd: input.worktreePath,
        } as const);
    }
  });

  const layer = Layer.mergeAll(
    makeInvocationLayer(options.capabilities ?? new Set(["preview", "worktree"])),
    Layer.mock(OrchestrationEngine.OrchestrationEngineService)({
      dispatch,
    } satisfies Partial<OrchestrationEngine.OrchestrationEngineService["Service"]>),
    Layer.mock(ProjectionSnapshotQuery.ProjectionSnapshotQuery)({
      getThreadDetailById: (id) =>
        Effect.succeed(id === threadId && thread ? Option.some(thread) : Option.none()),
      getProjectShellById: (id) =>
        Effect.succeed(id === projectId ? Option.some(projectShell) : Option.none()),
    } satisfies Partial<ProjectionSnapshotQuery.ProjectionSnapshotQuery["Service"]>),
    ServerSettings.ServerSettingsService.layerTest({
      newWorktreesStartFromOrigin: options.newWorktreesStartFromOrigin ?? false,
    }),
    Layer.mock(GitWorkflowService.GitWorkflowService)({
      localStatus,
      fetchRemote,
      resolveRemoteTrackingCommit,
      createWorktree,
      removeWorktree,
    } satisfies Partial<GitWorkflowService.GitWorkflowService["Service"]>),
    Layer.mock(ProjectSetupScriptRunner.ProjectSetupScriptRunner)({
      runForThread,
    } satisfies Partial<ProjectSetupScriptRunner.ProjectSetupScriptRunner["Service"]>),
    Layer.mock(VcsStatusBroadcaster)({
      refreshStatus,
    } satisfies Partial<VcsStatusBroadcaster["Service"]>),
  ).pipe(Layer.provideMerge(NodeServices.layer));

  return {
    layer,
    dispatch,
    fetchRemote,
    resolveRemoteTrackingCommit,
    createWorktree,
    removeWorktree,
    localStatus,
    runForThread,
  };
};

const expectTypedFailure = (exit: Exit.Exit<unknown, unknown>, expected: object): void => {
  if (!Exit.isFailure(exit)) {
    expect.fail(`Expected a failure exit, got: ${JSON.stringify(exit)}`);
  }
  const reason = exit.cause.reasons[0];
  if (reason?._tag !== "Fail") {
    expect.fail(`Expected a typed Fail cause, got: ${reason?._tag ?? "no reason"}`);
  }
  expect(reason.error).toMatchObject(expected);
};

const runHandoff = (
  harness: ReturnType<typeof makeHarness>,
  input: Parameters<typeof __testing.worktreeHandoff>[0],
) => __testing.worktreeHandoff(input).pipe(Effect.provide(harness.layer));

describe("worktree_handoff", () => {
  it.effect("creates a worktree from the current branch and re-points the thread", () => {
    const harness = makeHarness();
    return Effect.gen(function* () {
      const result = yield* runHandoff(harness, { branch: "feature/handoff" });

      expect(result.branch).toBe("feature/handoff");
      expect(result.baseRef).toBe("dev");
      expect(result.startedFromOrigin).toBe(false);
      expect(result.worktreePath).toBe("/worktrees/project/feature/handoff");
      expect(result.setupScript).toMatchObject({ status: "started", scriptName: "Setup" });

      expect(harness.fetchRemote).not.toHaveBeenCalled();
      expect(harness.createWorktree).toHaveBeenCalledWith({
        cwd: workspaceRoot,
        refName: "dev",
        newRefName: "feature/handoff",
        baseRefName: "dev",
        path: null,
      });
      expect(harness.dispatch).toHaveBeenCalledWith(
        expect.objectContaining({
          type: "thread.meta.update",
          threadId,
          branch: "feature/handoff",
          worktreePath: "/worktrees/project/feature/handoff",
        }),
      );
      expect(harness.runForThread).toHaveBeenCalledWith({
        threadId,
        projectId,
        projectCwd: workspaceRoot,
        worktreePath: "/worktrees/project/feature/handoff",
      });
    });
  });

  it.effect("starts from origin and honors explicit baseRef and path", () => {
    const harness = makeHarness();
    return Effect.gen(function* () {
      const result = yield* runHandoff(harness, {
        branch: "feature/from-origin",
        baseRef: "dev",
        startFromOrigin: true,
        path: "/custom/worktree/location",
        runSetupScript: false,
      });

      expect(harness.fetchRemote).toHaveBeenCalledWith({
        cwd: workspaceRoot,
        remoteName: "origin",
      });
      expect(harness.resolveRemoteTrackingCommit).toHaveBeenCalledWith({
        cwd: workspaceRoot,
        refName: "dev",
        fallbackRemoteName: "origin",
      });
      expect(harness.createWorktree).toHaveBeenCalledWith({
        cwd: workspaceRoot,
        refName: "abc123",
        newRefName: "feature/from-origin",
        baseRefName: "dev",
        path: "/custom/worktree/location",
      });
      expect(harness.localStatus).not.toHaveBeenCalled();
      expect(harness.runForThread).not.toHaveBeenCalled();
      expect(result.worktreePath).toBe("/custom/worktree/location");
      expect(result.startedFromOrigin).toBe(true);
      expect(result.setupScript).toEqual({ status: "skipped" });
    });
  });

  it.effect("uses the server setting for startFromOrigin when unspecified", () => {
    const harness = makeHarness({ newWorktreesStartFromOrigin: true });
    return Effect.gen(function* () {
      const result = yield* runHandoff(harness, { branch: "feature/settings-origin" });
      expect(result.startedFromOrigin).toBe(true);
      expect(harness.fetchRemote).toHaveBeenCalled();
    });
  });

  it.effect("fails when the thread is already attached to a worktree", () => {
    const harness = makeHarness({
      thread: makeThread({ worktreePath: "/worktrees/project/existing" }),
    });
    return Effect.gen(function* () {
      const exit = yield* Effect.exit(runHandoff(harness, { branch: "feature/second" }));
      expectTypedFailure(exit, {
        _tag: "WorktreeHandoffAlreadyInWorktreeError",
        worktreePath: "/worktrees/project/existing",
      });
      expect(harness.createWorktree).not.toHaveBeenCalled();
    });
  });

  it.effect("fails when the worktree capability is missing", () => {
    const harness = makeHarness({ capabilities: new Set(["preview"]) });
    return Effect.gen(function* () {
      const exit = yield* Effect.exit(runHandoff(harness, { branch: "feature/no-capability" }));
      expectTypedFailure(exit, { _tag: "WorktreeCapabilityUnavailableError" });
    });
  });

  it.effect("serializes concurrent handoffs for the same thread", () =>
    Effect.gen(function* () {
      const gate = yield* Deferred.make<void>();
      const harness = makeHarness({ createWorktreeGate: Deferred.await(gate) });

      // First handoff acquires the per-thread guard and blocks on the gate.
      const first = yield* Effect.forkChild(
        Effect.exit(runHandoff(harness, { branch: "feature/race-1" })),
      );
      yield* Effect.yieldNow;

      // Second handoff for the same thread must be refused while the first
      // is still in flight.
      const second = yield* Effect.exit(runHandoff(harness, { branch: "feature/race-2" }));
      expectTypedFailure(second, { _tag: "WorktreeHandoffInvalidRequestError" });

      yield* Deferred.succeed(gate, undefined);
      const firstExit = yield* Fiber.join(first);
      expect(Exit.isSuccess(firstExit)).toBe(true);
      expect(harness.createWorktree).toHaveBeenCalledTimes(1);
    }),
  );

  it.effect("removes the created worktree when the thread update fails", () => {
    const harness = makeHarness({ dispatchFails: true });
    return Effect.gen(function* () {
      const exit = yield* Effect.exit(runHandoff(harness, { branch: "feature/dispatch-fails" }));
      expectTypedFailure(exit, {
        _tag: "WorktreeOperationError",
        operation: "updateThreadMetadata",
      });
      expect(harness.createWorktree).toHaveBeenCalledTimes(1);
      expect(harness.removeWorktree).toHaveBeenCalledWith({
        cwd: workspaceRoot,
        path: "/worktrees/project/feature/dispatch-fails",
        force: true,
      });
    });
  });

  it.effect("rejects a relative path", () => {
    const harness = makeHarness();
    return Effect.gen(function* () {
      const exit = yield* Effect.exit(
        runHandoff(harness, { branch: "feature/relative-path", path: "worktrees/nested" }),
      );
      expectTypedFailure(exit, { _tag: "WorktreeHandoffInvalidRequestError" });
      expect(harness.createWorktree).not.toHaveBeenCalled();
    });
  });

  it.effect("fails when baseRef is omitted and HEAD is detached", () => {
    const harness = makeHarness({ currentBranch: null });
    return Effect.gen(function* () {
      const exit = yield* Effect.exit(runHandoff(harness, { branch: "feature/detached" }));
      expectTypedFailure(exit, { _tag: "WorktreeHandoffInvalidRequestError" });
    });
  });

  it.effect("reports setup script failure without failing the handoff", () => {
    const harness = makeHarness({ setupScript: "fails" });
    return Effect.gen(function* () {
      const result = yield* runHandoff(harness, { branch: "feature/setup-fails" });
      expect(result.setupScript.status).toBe("failed");
      expect(harness.dispatch).toHaveBeenCalled();
    });
  });

  it.effect("reports a setup script defect without failing the handoff", () => {
    const harness = makeHarness({ setupScript: "dies" });
    return Effect.gen(function* () {
      const result = yield* runHandoff(harness, { branch: "feature/setup-dies" });
      expect(result.setupScript).toEqual({ status: "failed", detail: "setup runner defect" });
      expect(harness.dispatch).toHaveBeenCalled();
    });
  });
});

describe("worktree_status", () => {
  it.effect("reports an unattached thread", () => {
    const harness = makeHarness({ newWorktreesStartFromOrigin: true });
    return Effect.gen(function* () {
      const result = yield* __testing.worktreeStatus().pipe(Effect.provide(harness.layer));
      expect(result).toEqual({
        attached: false,
        worktreePath: null,
        branch: null,
        projectWorkspaceRoot: workspaceRoot,
        defaultStartFromOrigin: true,
      });
    });
  });

  it.effect("reports an attached thread's worktree and branch", () => {
    const harness = makeHarness({
      thread: makeThread({
        worktreePath: "/worktrees/project/existing",
        branch: "feature/existing",
      }),
    });
    return Effect.gen(function* () {
      const result = yield* __testing.worktreeStatus().pipe(Effect.provide(harness.layer));
      expect(result).toMatchObject({
        attached: true,
        worktreePath: "/worktrees/project/existing",
        branch: "feature/existing",
        defaultStartFromOrigin: false,
      });
    });
  });

  it.effect("fails when the worktree capability is missing", () => {
    const harness = makeHarness({ capabilities: new Set(["preview"]) });
    return Effect.gen(function* () {
      const exit = yield* Effect.exit(
        __testing.worktreeStatus().pipe(Effect.provide(harness.layer)),
      );
      expectTypedFailure(exit, { _tag: "WorktreeCapabilityUnavailableError" });
    });
  });
});
