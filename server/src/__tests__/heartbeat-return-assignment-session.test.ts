import { randomUUID } from "node:crypto";
import { eq } from "drizzle-orm";
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
import {
  describeSessionResetReason,
  heartbeatService,
  resolveReturnAssignmentTaskSession,
  shouldResetTaskSessionForWake,
} from "../services/heartbeat.ts";
import { runningProcesses } from "../adapters/index.ts";

type FakeAdapterInput = {
  runId: string;
  agent: { id: string };
  runtime?: { sessionParams?: Record<string, unknown> | null };
  context: Record<string, unknown>;
};

const defaultResult = () => ({
  exitCode: 0,
  signal: null,
  timedOut: false,
  errorMessage: null,
  summary: "Return-assignment test run.",
  provider: "test",
  model: "test-model",
});

// Deterministic fake adapter: no provider process, no paid calls.
const mockAdapterExecute = vi.hoisted(() => vi.fn(async (_input: unknown): Promise<unknown> => ({
  exitCode: 0,
  signal: null,
  timedOut: false,
  errorMessage: null,
  summary: "Return-assignment test run.",
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
    `Skipping embedded Postgres return-assignment session tests on this host: ${embeddedPostgresSupport.reason ?? "unsupported environment"}`,
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

describe("return-assignment session policy", () => {
  it("keeps the assignment reset unless the assignee is returning", () => {
    expect(shouldResetTaskSessionForWake({ wakeReason: "issue_assigned" })).toBe(true);
    expect(shouldResetTaskSessionForWake({ wakeReason: "issue_assigned" }, { returningAssignee: true })).toBe(false);
    expect(describeSessionResetReason({ wakeReason: "issue_assigned" })).toBe("wake reason is issue_assigned");
    expect(describeSessionResetReason({ wakeReason: "issue_assigned" }, { returningAssignee: true })).toBeNull();
  });

  it("never overrides an explicit fresh-session request or other reset boundaries", () => {
    expect(shouldResetTaskSessionForWake(
      { wakeReason: "issue_assigned", forceFreshSession: true },
      { returningAssignee: true },
    )).toBe(true);
    expect(shouldResetTaskSessionForWake(
      { wakeReason: "execution_approval_requested" },
      { returningAssignee: true },
    )).toBe(true);
  });

  it("only treats a healthy existing session on an assignment wake as returning", () => {
    const healthy = { lastRunId: "run-1", lastError: null, updatedAt: new Date("2026-09-01T00:00:00.000Z") };
    expect(resolveReturnAssignmentTaskSession({ contextSnapshot: { wakeReason: "issue_assigned" }, taskSession: healthy }))
      .toEqual({ checkpointAt: healthy.updatedAt, lastRunId: "run-1" });
    expect(resolveReturnAssignmentTaskSession({ contextSnapshot: { wakeReason: "issue_assigned" }, taskSession: null }))
      .toBeNull();
    expect(resolveReturnAssignmentTaskSession({
      contextSnapshot: { wakeReason: "issue_assigned" },
      taskSession: { ...healthy, lastError: "adapter failed" },
    })).toBeNull();
    expect(resolveReturnAssignmentTaskSession({
      contextSnapshot: { wakeReason: "issue_assigned", forceFreshSession: true },
      taskSession: healthy,
    })).toBeNull();
    expect(resolveReturnAssignmentTaskSession({ contextSnapshot: { wakeReason: "issue_commented" }, taskSession: healthy }))
      .toBeNull();
  });
});

describeEmbeddedPostgres("return-assignment session reuse across agents", () => {
  let db!: ReturnType<typeof createDb>;
  let heartbeat!: ReturnType<typeof heartbeatService>;
  let tempDb: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>> | null = null;

  beforeAll(async () => {
    tempDb = await startEmbeddedPostgresTestDatabase("paperclip-heartbeat-return-assignment-");
    db = createDb(tempDb.connectionString);
    heartbeat = heartbeatService(db);
  }, 20_000);

  afterEach(async () => {
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

  async function seed() {
    const companyId = randomUUID();
    const originalId = randomUUID();
    const interimId = randomUUID();
    const issueId = randomUUID();
    await db.insert(companies).values({
      id: companyId,
      name: "Paperclip",
      issuePrefix: `T${companyId.replace(/-/g, "").slice(0, 6).toUpperCase()}`,
      requireBoardApprovalForNewAgents: false,
      defaultResponsibleUserId: "responsible-user",
    });
    for (const [id, name] of [[originalId, "OriginalCoder"], [interimId, "InterimCoder"]] as const) {
      await db.insert(agents).values({
        id,
        companyId,
        name,
        role: "engineer",
        status: "active",
        adapterType: "codex_local",
        adapterConfig: { model: "gpt-5.4-mini" },
        runtimeConfig: { heartbeat: { wakeOnDemand: true, maxConcurrentRuns: 1 } },
        permissions: {},
      });
    }
    await db.insert(issues).values({
      id: issueId,
      companyId,
      title: "Hand a task away and back",
      status: "todo",
      priority: "high",
      assigneeAgentId: originalId,
      responsibleUserId: "responsible-user",
    });
    return { companyId, originalId, interimId, issueId };
  }

  type Ids = Awaited<ReturnType<typeof seed>>;

  function adapterInputs() {
    return mockAdapterExecute.mock.calls.map((call) => (call as unknown[])[0] as FakeAdapterInput);
  }

  function adapterInputFor(runId: string) {
    return adapterInputs().find((input) => input.runId === runId) ?? null;
  }

  // Fake adapter turn: posts the agent's progress comment from inside the run
  // (as a real agent would) and reports the given session.
  function agentTurn(ids: Ids, body: string, sessionId: string | null) {
    return async (input: unknown) => {
      const { runId, agent } = input as FakeAdapterInput;
      await db.insert(issueComments).values({
        companyId: ids.companyId,
        issueId: ids.issueId,
        authorAgentId: agent.id,
        authorType: "agent",
        createdByRunId: runId,
        body,
      });
      return sessionId
        ? { ...defaultResult(), sessionParams: { sessionId }, sessionDisplayId: sessionId }
        : defaultResult();
    };
  }

  // Mirrors a board reassignment: the issue row changes and the activity log
  // records it (the re-wake throttle counts that entry as new input).
  async function assignAndWake(ids: Ids, agentId: string, extraContext: Record<string, unknown> = {}) {
    await db.update(issues).set({ assigneeAgentId: agentId, updatedAt: new Date() }).where(eq(issues.id, ids.issueId));
    await db.insert(activityLog).values({
      companyId: ids.companyId,
      actorType: "user",
      actorId: "user-1",
      action: "issue.updated",
      entityType: "issue",
      entityId: ids.issueId,
      details: { assigneeAgentId: agentId },
    });
    const run = await heartbeat.wakeup(agentId, {
      source: "assignment",
      triggerDetail: "system",
      reason: "issue_assigned",
      payload: { issueId: ids.issueId },
      contextSnapshot: { issueId: ids.issueId, taskId: ids.issueId, wakeReason: "issue_assigned", ...extraContext },
    });
    expect(run).not.toBeNull();
    return run!;
  }

  async function waitForIdle(ids: Ids) {
    expect(await waitForCondition(async () => {
      const issue = await db
        .select({ executionRunId: issues.executionRunId })
        .from(issues)
        .where(eq(issues.id, ids.issueId))
        .then((rows) => rows[0]);
      const busyAgents = await db
        .select({ status: agents.status })
        .from(agents)
        .where(eq(agents.companyId, ids.companyId));
      const runs = await db
        .select({ status: heartbeatRuns.status })
        .from(heartbeatRuns)
        .where(eq(heartbeatRuns.companyId, ids.companyId));
      return issue?.executionRunId == null
        && busyAgents.every((agent) => agent.status !== "running")
        && runs.every((run) => run.status !== "queued" && run.status !== "running");
    })).toBe(true);
  }

  async function runToIdle(ids: Ids, runId: string) {
    expect(await waitForCondition(async () => adapterInputFor(runId) !== null)).toBe(true);
    await waitForIdle(ids);
  }

  async function userComment(ids: Ids, body: string) {
    await db.insert(issueComments).values({
      companyId: ids.companyId,
      issueId: ids.issueId,
      authorUserId: "user-1",
      authorType: "user",
      body,
    });
  }

  function wakeCommentBodies(input: FakeAdapterInput | null) {
    const wake = (input?.context.paperclipWake ?? {}) as { comments?: Array<{ body?: string }> };
    return (wake.comments ?? []).map((comment) => comment.body);
  }

  async function handAwayAndBack(ids: Ids) {
    mockAdapterExecute.mockImplementationOnce(agentTurn(ids, "Original: first pass done.", "session-original"));
    const first = await assignAndWake(ids, ids.originalId);
    await runToIdle(ids, first.id);

    mockAdapterExecute.mockImplementationOnce(agentTurn(ids, "Interim: found the flaky test.", "session-interim"));
    const interim = await assignAndWake(ids, ids.interimId);
    await runToIdle(ids, interim.id);
    await userComment(ids, "User: please also update the changelog.");
  }

  it("resumes the returning assignee's session and delivers what changed while it was away", async () => {
    const ids = await seed();
    await handAwayAndBack(ids);

    const back = await assignAndWake(ids, ids.originalId);
    await runToIdle(ids, back.id);

    const input = adapterInputFor(back.id);
    expect(input?.runtime?.sessionParams?.sessionId).toBe("session-original");
    // Everything other authors posted while it was away (the interim agent,
    // the user, and any system notices), oldest first; nothing of its own.
    const bodies = wakeCommentBodies(input);
    expect(bodies).toEqual(expect.arrayContaining([
      "Interim: found the flaky test.",
      "User: please also update the changelog.",
    ]));
    expect(bodies.at(-1)).toBe("User: please also update the changelog.");
    expect(bodies).not.toContain("Original: first pass done.");
    expect(input?.context.paperclipReturnAssignment).toMatchObject({
      interveningCommentsTruncated: false,
    });
    expect((input?.context.paperclipWake as { fallbackFetchNeeded?: boolean }).fallbackFetchNeeded).toBe(false);

    const run = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, back.id)).then((rows) => rows[0]!);
    expect(run.sessionIdBefore).toBe("session-original");
    expect(run.resultJson).toMatchObject({
      configFreshness: { session: { returningAssignee: true, taskSessionReused: true, reset: false } },
    });
  });

  it("also delivers comments posted while its last run was still in flight", async () => {
    const ids = await seed();
    await handAwayAndBack(ids);
    const session = await db
      .select()
      .from(agentTaskSessions)
      .where(eq(agentTaskSessions.agentId, ids.originalId))
      .then((rows) => rows[0]!);
    const checkpointRun = await db
      .select({ startedAt: heartbeatRuns.startedAt })
      .from(heartbeatRuns)
      .where(eq(heartbeatRuns.id, session.lastRunId!))
      .then((rows) => rows[0]!);
    // Posted after that run started but before it checkpointed, so no wake
    // payload of the returning agent has carried it.
    const midRun = new Date(checkpointRun.startedAt!.getTime() + 1);
    expect(midRun.getTime()).toBeLessThan(session.updatedAt.getTime());
    await db.insert(issueComments).values({
      companyId: ids.companyId,
      issueId: ids.issueId,
      authorUserId: "user-1",
      authorType: "user",
      body: "User: posted mid-run.",
      createdAt: midRun,
    });

    const back = await assignAndWake(ids, ids.originalId);
    await runToIdle(ids, back.id);
    expect(adapterInputFor(back.id)?.runtime?.sessionParams?.sessionId).toBe("session-original");
    expect(wakeCommentBodies(adapterInputFor(back.id))).toContain("User: posted mid-run.");
  });

  it("still starts fresh when the returning wake asks for a fresh session", async () => {
    const ids = await seed();
    await handAwayAndBack(ids);

    const back = await assignAndWake(ids, ids.originalId, { forceFreshSession: true });
    await runToIdle(ids, back.id);

    expect(adapterInputFor(back.id)?.runtime?.sessionParams ?? null).toBeNull();
    expect(adapterInputFor(back.id)?.context.paperclipReturnAssignment).toBeUndefined();
  });

  it("still starts fresh when the returning agent's configuration changed while it was away", async () => {
    const ids = await seed();
    await handAwayAndBack(ids);
    await db.update(agents).set({ adapterConfig: { model: "gpt-5.4" } }).where(eq(agents.id, ids.originalId));

    const back = await assignAndWake(ids, ids.originalId);
    await runToIdle(ids, back.id);

    expect(adapterInputFor(back.id)?.runtime?.sessionParams ?? null).toBeNull();
    const run = await db.select().from(heartbeatRuns).where(eq(heartbeatRuns.id, back.id)).then((rows) => rows[0]!);
    expect(run.resultJson).toMatchObject({
      configFreshness: { session: { returningAssignee: true, taskSessionReused: false, reset: true } },
    });
  });

  it("starts fresh when the returning agent's session ended in a failure", async () => {
    const ids = await seed();
    await handAwayAndBack(ids);
    // The state a failed, not-yet-retried run leaves: session kept, lastError set.
    await db
      .update(agentTaskSessions)
      .set({ lastError: "synthetic adapter failure" })
      .where(eq(agentTaskSessions.agentId, ids.originalId));

    const back = await assignAndWake(ids, ids.originalId);
    await runToIdle(ids, back.id);
    expect(adapterInputFor(back.id)?.runtime?.sessionParams ?? null).toBeNull();
    expect(adapterInputFor(back.id)?.context.paperclipReturnAssignment).toBeUndefined();
  });

  it("leaves a first assignment unchanged", async () => {
    const ids = await seed();
    mockAdapterExecute.mockImplementationOnce(agentTurn(ids, "Original: first pass done.", "session-original"));
    const first = await assignAndWake(ids, ids.originalId);
    await runToIdle(ids, first.id);

    const input = adapterInputFor(first.id);
    expect(input?.runtime?.sessionParams ?? null).toBeNull();
    expect(input?.context.paperclipReturnAssignment).toBeUndefined();
  });

  it("inlines only the newest intervening comments and asks the agent to fetch the rest", async () => {
    const ids = await seed();
    await handAwayAndBack(ids);
    for (let index = 1; index <= 10; index += 1) {
      await userComment(ids, `User: note ${index}`);
    }

    const back = await assignAndWake(ids, ids.originalId);
    await runToIdle(ids, back.id);

    const input = adapterInputFor(back.id);
    expect(input?.runtime?.sessionParams?.sessionId).toBe("session-original");
    const bodies = wakeCommentBodies(input);
    expect(bodies).toHaveLength(8);
    expect(bodies.at(-1)).toBe("User: note 10");
    expect(bodies).not.toContain("Interim: found the flaky test.");
    expect((input?.context.paperclipWake as { fallbackFetchNeeded?: boolean }).fallbackFetchNeeded).toBe(true);
  });
});
