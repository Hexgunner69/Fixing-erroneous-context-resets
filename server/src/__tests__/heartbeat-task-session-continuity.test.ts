import { randomUUID } from "node:crypto";
import { eq, sql } from "drizzle-orm";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import {
  activityLog,
  agents,
  agentRuntimeState,
  agentTaskSessions,
  agentWakeupRequests,
  companySkills,
  companies,
  createDb,
  documentRevisions,
  documents,
  environmentLeases,
  environments,
  executionWorkspaces,
  heartbeatRunEvents,
  heartbeatRuns,
  issueComments,
  issueDocuments,
  issueRelations,
  issueTreeHolds,
  issues,
  workspaceOperations,
} from "@paperclipai/db";
import {
  getEmbeddedPostgresTestSupport,
  startEmbeddedPostgresTestDatabase,
} from "./helpers/embedded-postgres.js";
import { heartbeatService } from "../services/heartbeat.ts";
import { runningProcesses } from "../adapters/index.ts";

type FakeAdapterResult = {
  exitCode: number;
  signal: null;
  timedOut: boolean;
  errorMessage: string | null;
  summary: string;
  provider: string;
  model: string;
  sessionParams?: Record<string, unknown> | null;
  sessionDisplayId?: string | null;
};

const defaultResult = (): FakeAdapterResult => ({
  exitCode: 0,
  signal: null,
  timedOut: false,
  errorMessage: null,
  summary: "Task-session continuity test run.",
  provider: "test",
  model: "test-model",
});

// Deterministic fake adapter: no provider process, no paid calls.
const mockAdapterExecute = vi.hoisted(() => vi.fn(async (): Promise<unknown> => ({
  exitCode: 0,
  signal: null,
  timedOut: false,
  errorMessage: null,
  summary: "Task-session continuity test run.",
  provider: "test",
  model: "test-model",
})));

vi.mock("../adapters/index.ts", async () => {
  const actual = await vi.importActual<typeof import("../adapters/index.ts")>("../adapters/index.ts");
  return {
    ...actual,
    getServerAdapter: vi.fn(() => ({
      supportsLocalAgentJwt: false,
      execute: mockAdapterExecute,
    })),
  };
});

const embeddedPostgresSupport = await getEmbeddedPostgresTestSupport();
const describeEmbeddedPostgres = embeddedPostgresSupport.supported ? describe : describe.skip;

if (!embeddedPostgresSupport.supported) {
  console.warn(
    `Skipping embedded Postgres heartbeat task-session continuity tests on this host: ${embeddedPostgresSupport.reason ?? "unsupported environment"}`,
  );
}

async function waitForCondition(fn: () => Promise<boolean>, timeoutMs = 30_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await fn()) return true;
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  return fn();
}

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

function sessionResult(sessionId: string): FakeAdapterResult {
  return { ...defaultResult(), sessionParams: { sessionId }, sessionDisplayId: sessionId };
}

type FakeAdapterInput = { runId: string; runtime?: { sessionParams?: Record<string, unknown> | null } };

function adapterInputs() {
  return mockAdapterExecute.mock.calls.map((call) => (call as unknown[])[0] as FakeAdapterInput);
}

function adapterWasCalledFor(runId: string) {
  return adapterInputs().some((input) => input.runId === runId);
}

// The session the fake adapter was asked to resume for `runId`.
function resumedSessionId(runId: string) {
  const params = adapterInputs().find((input) => input.runId === runId)?.runtime?.sessionParams;
  return typeof params?.sessionId === "string" ? params.sessionId : null;
}

describeEmbeddedPostgres("heartbeat task-session continuity across queued follow-ups", () => {
  let db!: ReturnType<typeof createDb>;
  let heartbeat!: ReturnType<typeof heartbeatService>;
  let tempDb: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>> | null = null;

  beforeAll(async () => {
    tempDb = await startEmbeddedPostgresTestDatabase("paperclip-heartbeat-task-session-continuity-");
    db = createDb(tempDb.connectionString);
    heartbeat = heartbeatService(db);
  }, 20_000);

  afterEach(async () => {
    await db.execute(sql.raw(`
      DROP TRIGGER IF EXISTS test_task_session_trigger ON agent_task_sessions;
      DROP FUNCTION IF EXISTS test_task_session_trigger_fn();
    `));
    await waitForCondition(async () => {
      const runs = await db.select({ status: heartbeatRuns.status }).from(heartbeatRuns);
      return runs.every((run) => run.status !== "queued" && run.status !== "running");
    }, 10_000);
    const runIds = await db
      .select({ id: heartbeatRuns.id })
      .from(heartbeatRuns)
      .then((runs) => runs.map((run) => run.id));
    await Promise.all(runIds.map((runId) => heartbeat.waitForRunExecutionDrain(runId)));
    mockAdapterExecute.mockReset();
    mockAdapterExecute.mockImplementation(async () => defaultResult());
    runningProcesses.clear();
    await db.delete(environmentLeases);
    await db.delete(activityLog);
    await db.delete(companySkills);
    await db.delete(issueComments);
    await db.delete(issueDocuments);
    await db.delete(documentRevisions);
    await db.delete(documents);
    await db.delete(issueRelations);
    await db.delete(issueTreeHolds);
    await db.delete(issues);
    await db.delete(heartbeatRunEvents);
    await db.delete(activityLog);
    await db.delete(agentTaskSessions);
    await db.delete(heartbeatRuns);
    await db.delete(agentWakeupRequests);
    await db.delete(agentRuntimeState);
    await db.delete(agents);
    await db.delete(environments);
    await db.delete(workspaceOperations);
    await db.delete(executionWorkspaces);
    for (let attempt = 0; attempt < 5; attempt += 1) {
      try {
        await db.transaction(async (tx) => {
          await tx.delete(companySkills);
          await tx.delete(companies);
        });
        break;
      } catch (error) {
        if (attempt === 4) throw error;
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
    }
  });

  afterAll(async () => {
    await tempDb?.cleanup();
  });

  async function seedAssignedIssue(runtimeConfig: Record<string, unknown> = {}) {
    const companyId = randomUUID();
    const agentId = randomUUID();
    const issueId = randomUUID();
    await db.insert(companies).values({
      id: companyId,
      name: "Paperclip",
      issuePrefix: `T${companyId.replace(/-/g, "").slice(0, 6).toUpperCase()}`,
      requireBoardApprovalForNewAgents: false,
      defaultResponsibleUserId: "responsible-user",
    });
    await db.insert(agents).values({
      id: agentId,
      companyId,
      name: "SessionCoder",
      role: "engineer",
      status: "active",
      adapterType: "codex_local",
      adapterConfig: {},
      runtimeConfig: { heartbeat: { wakeOnDemand: true, maxConcurrentRuns: 1 }, ...runtimeConfig },
      permissions: {},
    });
    await db.insert(issues).values({
      id: issueId,
      companyId,
      title: "Keep one resumable session per task",
      status: "todo",
      priority: "high",
      assigneeAgentId: agentId,
      responsibleUserId: "responsible-user",
    });
    return { companyId, agentId, issueId };
  }

  async function wakeAssigned(ids: { agentId: string; issueId: string }) {
    return heartbeat.wakeup(ids.agentId, {
      source: "assignment",
      triggerDetail: "system",
      reason: "issue_assigned",
      payload: { issueId: ids.issueId },
      contextSnapshot: { issueId: ids.issueId, taskId: ids.issueId, wakeReason: "issue_assigned" },
    });
  }

  async function wakeOnUserComment(
    ids: { companyId: string; agentId: string; issueId: string },
    body: string,
    extraContext: Record<string, unknown> = {},
  ) {
    const comment = await db
      .insert(issueComments)
      .values({ companyId: ids.companyId, issueId: ids.issueId, authorUserId: "user-1", authorType: "user", body })
      .returning()
      .then((rows) => rows[0]!);
    await db.update(issues).set({ updatedAt: new Date(Date.now() + 1_000) }).where(eq(issues.id, ids.issueId));
    return heartbeat.wakeup(ids.agentId, {
      source: "automation",
      triggerDetail: "system",
      reason: "issue_commented",
      payload: { issueId: ids.issueId, commentId: comment.id },
      contextSnapshot: {
        issueId: ids.issueId,
        taskId: ids.issueId,
        commentId: comment.id,
        wakeCommentId: comment.id,
        wakeReason: "issue_commented",
        ...extraContext,
      },
      requestedByActorType: "user",
      requestedByActorId: "user-1",
    });
  }

  async function recordAgentReply(ids: { companyId: string; agentId: string; issueId: string }, runId: string) {
    await db.insert(issueComments).values({
      companyId: ids.companyId,
      issueId: ids.issueId,
      authorAgentId: ids.agentId,
      authorType: "agent",
      createdByRunId: runId,
      body: "Agent progress update.",
    });
  }

  async function runStatus(runId: string) {
    return db
      .select({ status: heartbeatRuns.status })
      .from(heartbeatRuns)
      .where(eq(heartbeatRuns.id, runId))
      .then((rows) => rows[0]?.status ?? null);
  }

  async function taskSession(issueId: string) {
    return db
      .select()
      .from(agentTaskSessions)
      .where(eq(agentTaskSessions.taskKey, issueId))
      .then((rows) => rows[0] ?? null);
  }

  async function waitForIdle(ids: { agentId: string; issueId: string }) {
    expect(await waitForCondition(async () => {
      const issue = await db
        .select({ executionRunId: issues.executionRunId })
        .from(issues)
        .where(eq(issues.id, ids.issueId))
        .then((rows) => rows[0]);
      const agent = await db
        .select({ status: agents.status })
        .from(agents)
        .where(eq(agents.id, ids.agentId))
        .then((rows) => rows[0]);
      const active = await db
        .select({ status: heartbeatRuns.status })
        .from(heartbeatRuns)
        .where(eq(heartbeatRuns.agentId, ids.agentId));
      return issue?.executionRunId == null
        && agent?.status !== "running"
        && active.every((run) => run.status !== "queued" && run.status !== "running");
    })).toBe(true);
  }

  // Slow every task-session write so a follow-up dispatched before the write
  // commits would observe "no session" (the reported 16:04:34 race).
  async function delayTaskSessionWrites(seconds = 0.4) {
    await db.execute(sql.raw(`
      CREATE OR REPLACE FUNCTION test_task_session_trigger_fn()
      RETURNS trigger AS $$
      BEGIN
        PERFORM pg_sleep(${seconds});
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;
      CREATE TRIGGER test_task_session_trigger
      BEFORE INSERT OR UPDATE ON agent_task_sessions
      FOR EACH ROW EXECUTE FUNCTION test_task_session_trigger_fn();
    `));
  }

  async function establishSession(ids: { companyId: string; agentId: string; issueId: string }, sessionId: string) {
    mockAdapterExecute.mockImplementationOnce(async () => sessionResult(sessionId));
    const run = await wakeAssigned(ids);
    expect(run).not.toBeNull();
    expect(await waitForCondition(async () => (await runStatus(run!.id)) === "succeeded")).toBe(true);
    await recordAgentReply(ids, run!.id);
    await waitForIdle(ids);
    expect((await taskSession(ids.issueId))?.sessionDisplayId).toBe(sessionId);
    return run!;
  }

  it("resumes the predecessor's session in a queued same-task follow-up", async () => {
    const ids = await seedAssignedIssue();
    const first = deferred();
    mockAdapterExecute.mockImplementationOnce(async () => {
      await first.promise;
      return sessionResult("session-a");
    });
    mockAdapterExecute.mockImplementationOnce(async () => sessionResult("session-a"));

    const runA = await wakeAssigned(ids);
    expect(runA).not.toBeNull();
    expect(await waitForCondition(async () => adapterWasCalledFor(runA!.id))).toBe(true);
    await recordAgentReply(ids, runA!.id);

    // Same-task follow-up arrives while A is still running: it is deferred.
    expect(await wakeOnUserComment(ids, "Continue with this extra detail.")).toBeNull();

    await delayTaskSessionWrites();
    let sawTerminalWithoutSession = false;
    const watcher = (async () => {
      while ((await runStatus(runA!.id)) === "running") {
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
      const row = await taskSession(ids.issueId);
      if (row?.lastRunId !== runA!.id) sawTerminalWithoutSession = true;
    })();
    first.resolve();

    await watcher;
    await waitForIdle(ids);

    const runs = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.agentId, ids.agentId));
    const runB = runs.find((run) =>
      (run.contextSnapshot as Record<string, unknown> | null)?.wakeReason === "issue_commented");
    expect(runB).toBeDefined();
    expect(adapterWasCalledFor(runB!.id)).toBe(true);
    expect(resumedSessionId(runB!.id)).toBe("session-a");
    expect(sawTerminalWithoutSession).toBe(false);
    expect(runB!.status).toBe("succeeded");
    expect(runB!.sessionIdBefore).toBe("session-a");
    const finalSession = await taskSession(ids.issueId);
    expect(finalSession?.sessionDisplayId).toBe("session-a");
    expect(finalSession?.lastRunId).not.toBe(runA!.id);
  }, 60_000);

  it("preserves the previous session atomically when the adapter throws", async () => {
    const ids = await seedAssignedIssue();
    await establishSession(ids, "session-kept-after-error");

    await delayTaskSessionWrites();
    mockAdapterExecute.mockImplementationOnce(async () => {
      throw new Error("synthetic adapter failure after resume");
    });
    const failedRun = await wakeOnUserComment(ids, "Trigger the adapter error.");
    expect(failedRun).not.toBeNull();

    let sawTerminalWithoutSession = false;
    expect(await waitForCondition(async () => {
      const status = await runStatus(failedRun!.id);
      if (status !== "failed") return false;
      if ((await taskSession(ids.issueId))?.lastRunId !== failedRun!.id) sawTerminalWithoutSession = true;
      return true;
    })).toBe(true);
    expect(sawTerminalWithoutSession).toBe(false);
    expect(resumedSessionId(failedRun!.id)).toBe("session-kept-after-error");
    expect(await taskSession(ids.issueId)).toMatchObject({
      sessionDisplayId: "session-kept-after-error",
      lastRunId: failedRun!.id,
      lastError: "synthetic adapter failure after resume",
    });
  }, 60_000);

  it("fails the run and keeps the previous session when the checkpoint cannot be persisted", async () => {
    const ids = await seedAssignedIssue();
    await establishSession(ids, "session-before-poison");

    await db.execute(sql.raw(`
      CREATE OR REPLACE FUNCTION test_task_session_trigger_fn()
      RETURNS trigger AS $$
      BEGIN
        IF NEW.session_display_id = 'poison-session' THEN
          RAISE EXCEPTION 'synthetic task-session persistence failure';
        END IF;
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;
      CREATE TRIGGER test_task_session_trigger
      BEFORE INSERT OR UPDATE ON agent_task_sessions
      FOR EACH ROW EXECUTE FUNCTION test_task_session_trigger_fn();
    `));
    mockAdapterExecute.mockImplementationOnce(async () => sessionResult("poison-session"));
    const poisonRun = await wakeOnUserComment(ids, "This run's checkpoint will fail to persist.");
    expect(poisonRun).not.toBeNull();
    expect(await waitForCondition(async () => {
      const status = await runStatus(poisonRun!.id);
      return status !== "running" && status !== "queued";
    })).toBe(true);

    // The run never became "succeeded" without its checkpoint.
    expect(await runStatus(poisonRun!.id)).toBe("failed");
    expect(await taskSession(ids.issueId)).toMatchObject({ sessionDisplayId: "session-before-poison" });

    await waitForIdle(ids);
    mockAdapterExecute.mockImplementationOnce(async () => sessionResult("session-before-poison"));
    const next = await wakeOnUserComment(ids, "Follow up after the persistence failure.");
    expect(next).not.toBeNull();
    expect(await waitForCondition(async () => adapterWasCalledFor(next!.id))).toBe(true);
    expect(resumedSessionId(next!.id)).toBe("session-before-poison");
  }, 60_000);

  it("does not let an in-flight run resurrect a session the operator reset", async () => {
    const ids = await seedAssignedIssue();
    await establishSession(ids, "session-to-reset");

    const inFlight = deferred();
    mockAdapterExecute.mockImplementationOnce(async () => {
      await inFlight.promise;
      return sessionResult("session-to-reset");
    });
    const runB = await wakeOnUserComment(ids, "Start a run that outlives the reset.");
    expect(runB).not.toBeNull();
    expect(await waitForCondition(async () => adapterWasCalledFor(runB!.id))).toBe(true);
    expect(resumedSessionId(runB!.id)).toBe("session-to-reset");

    const reset = await heartbeat.resetRuntimeSession(ids.agentId, { taskKey: ids.issueId });
    expect(reset?.clearedTaskSessions).toBe(1);
    inFlight.resolve();
    expect(await waitForCondition(async () => (await runStatus(runB!.id)) === "succeeded")).toBe(true);
    await recordAgentReply(ids, runB!.id);
    await waitForIdle(ids);

    expect(await heartbeat.listTaskSessions(ids.agentId)).toHaveLength(0);
    mockAdapterExecute.mockImplementationOnce(async () => sessionResult("session-after-reset"));
    const runC = await wakeOnUserComment(ids, "First run after the reset.");
    expect(runC).not.toBeNull();
    expect(await waitForCondition(async () => adapterWasCalledFor(runC!.id))).toBe(true);
    expect(resumedSessionId(runC!.id)).toBeNull();
    expect(await waitForCondition(async () =>
      (await taskSession(ids.issueId))?.sessionDisplayId === "session-after-reset")).toBe(true);
  }, 60_000);

  it("does not checkpoint a cancelled run's late adapter result", async () => {
    const ids = await seedAssignedIssue();
    await establishSession(ids, "session-before-cancel");

    const inFlight = deferred();
    mockAdapterExecute.mockImplementationOnce(async () => {
      await inFlight.promise;
      return sessionResult("session-from-cancelled-run");
    });
    const runB = await wakeOnUserComment(ids, "Start a run that will be cancelled.");
    expect(runB).not.toBeNull();
    expect(await waitForCondition(async () => adapterWasCalledFor(runB!.id))).toBe(true);

    await heartbeat.cancelRun(runB!.id);
    expect(await runStatus(runB!.id)).toBe("cancelled");
    inFlight.resolve();
    await heartbeat.waitForRunExecutionDrain(runB!.id);

    expect(await runStatus(runB!.id)).toBe("cancelled");
    await waitForIdle(ids);
    // The cancelled run's late result is dropped; any follow-up the cancel
    // triggers resumes the pre-cancel session instead.
    const session = await taskSession(ids.issueId);
    expect(session?.sessionDisplayId).toBe("session-before-cancel");
    expect(session?.lastRunId).not.toBe(runB!.id);
    const followUps = adapterInputs().filter((input) => input.runId !== runB!.id).slice(1);
    for (const input of followUps) {
      expect(resumedSessionId(input.runId)).not.toBe("session-from-cancelled-run");
    }
  }, 60_000);

  it("resumes the session when a follow-up requests a model profile the agent has disabled", async () => {
    const ids = await seedAssignedIssue({ modelProfiles: { cheap: { enabled: false } } });
    await establishSession(ids, "session-before-cheap-request");

    mockAdapterExecute.mockImplementationOnce(async () => sessionResult("session-before-cheap-request"));
    const run = await wakeOnUserComment(ids, "Continue, cheaply if you can.", { modelProfile: "cheap" });
    expect(run).not.toBeNull();
    expect(await waitForCondition(async () => adapterWasCalledFor(run!.id))).toBe(true);
    expect(resumedSessionId(run!.id)).toBe("session-before-cheap-request");

    await waitForIdle(ids);
    const finished = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, run!.id)).then((rows) => rows[0]!);
    // The request is still recorded for audit; it just no longer forks the session.
    expect(finished.resultJson).toMatchObject({
      modelProfile: { requested: "cheap", applied: null, fallbackReason: "agent_runtime_profile_disabled" },
      configFreshness: { session: { reset: false, taskSessionReused: true } },
    });
  }, 60_000);
});
