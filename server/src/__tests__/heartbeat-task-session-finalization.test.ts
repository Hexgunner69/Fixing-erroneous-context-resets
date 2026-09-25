import { randomUUID } from "node:crypto";
import { eq, sql } from "drizzle-orm";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import {
  agents,
  agentRuntimeState,
  agentTaskSessions,
  companies,
  createDb,
  heartbeatRuns,
} from "@paperclipai/db";
import {
  finalizeRunningRunWithTaskSession,
  heartbeatService,
  type FinalizeRunTaskSessionMutation,
} from "../services/heartbeat.ts";
import {
  getEmbeddedPostgresTestSupport,
  startEmbeddedPostgresTestDatabase,
} from "./helpers/embedded-postgres.js";

const embeddedPostgresSupport = await getEmbeddedPostgresTestSupport();
const describeEmbeddedPostgres = embeddedPostgresSupport.supported ? describe : describe.skip;

if (!embeddedPostgresSupport.supported) {
  console.warn(
    `Skipping embedded Postgres heartbeat task-session finalization tests on this host: ${embeddedPostgresSupport.reason ?? "unsupported environment"}`,
  );
}

const ADAPTER = "codex_local";
const CANCELLABLE = ["queued", "running", "scheduled_retry"] as const;

describeEmbeddedPostgres("heartbeat task-session finalization", () => {
  let tempDb: Awaited<ReturnType<typeof startEmbeddedPostgresTestDatabase>> | null = null;
  let db!: ReturnType<typeof createDb>;
  let observerDb!: ReturnType<typeof createDb>;

  beforeAll(async () => {
    tempDb = await startEmbeddedPostgresTestDatabase("paperclip-task-session-finalization-");
    db = createDb(tempDb.connectionString);
    observerDb = createDb(tempDb.connectionString);
  }, 20_000);

  afterEach(async () => {
    await db.execute(sql.raw(`
      DROP TRIGGER IF EXISTS test_task_session_trigger ON agent_task_sessions;
      DROP FUNCTION IF EXISTS test_task_session_trigger_fn();
    `));
    await db.delete(agentTaskSessions);
    await db.delete(agentRuntimeState);
    await db.delete(heartbeatRuns);
    await db.delete(agents);
    await db.delete(companies);
  });

  afterAll(async () => {
    await observerDb?.$client?.end?.({ timeout: 0 });
    await db?.$client?.end?.({ timeout: 0 });
    await tempDb?.cleanup();
  });

  async function seed() {
    const companyId = randomUUID();
    const agentId = randomUUID();
    const taskKey = randomUUID();
    await db.insert(companies).values({
      id: companyId,
      name: "Paperclip",
      issuePrefix: `T${companyId.replace(/-/g, "").slice(0, 6).toUpperCase()}`,
      requireBoardApprovalForNewAgents: false,
    });
    await db.insert(agents).values({
      id: agentId,
      companyId,
      name: "Session Finalizer",
      role: "engineer",
      status: "running",
      adapterType: ADAPTER,
      adapterConfig: {},
      runtimeConfig: {},
      permissions: {},
    });
    return { companyId, agentId, taskKey };
  }

  async function insertRun(
    ids: { companyId: string; agentId: string; taskKey: string },
    opts: { status?: string; startedAt?: Date; id?: string } = {},
  ) {
    const id = opts.id ?? randomUUID();
    const startedAt = opts.startedAt ?? new Date();
    await db.insert(heartbeatRuns).values({
      id,
      companyId: ids.companyId,
      agentId: ids.agentId,
      status: opts.status ?? "running",
      invocationSource: "on_demand",
      startedAt,
      createdAt: startedAt,
      contextSnapshot: { issueId: ids.taskKey, taskId: ids.taskKey },
    });
    return id;
  }

  function upsert(
    ids: { companyId: string; agentId: string; taskKey: string },
    sessionId: string,
  ): FinalizeRunTaskSessionMutation {
    return {
      kind: "upsert",
      companyId: ids.companyId,
      agentId: ids.agentId,
      adapterType: ADAPTER,
      taskKey: ids.taskKey,
      sessionParamsJson: { sessionId },
      sessionDisplayId: sessionId,
      lastError: null,
    };
  }

  function clear(ids: { companyId: string; agentId: string; taskKey: string }): FinalizeRunTaskSessionMutation {
    return { kind: "clear", companyId: ids.companyId, agentId: ids.agentId, adapterType: ADAPTER, taskKey: ids.taskKey };
  }

  async function sessionRow(taskKey: string) {
    return observerDb
      .select()
      .from(agentTaskSessions)
      .where(eq(agentTaskSessions.taskKey, taskKey))
      .then((rows) => rows[0] ?? null);
  }

  async function runStatus(runId: string) {
    return observerDb
      .select({ status: heartbeatRuns.status })
      .from(heartbeatRuns)
      .where(eq(heartbeatRuns.id, runId))
      .then((rows) => rows[0]?.status ?? null);
  }

  async function installSessionTrigger(body: string) {
    await db.execute(sql.raw(`
      CREATE OR REPLACE FUNCTION test_task_session_trigger_fn()
      RETURNS trigger AS $$
      BEGIN
        ${body}
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;
    `));
    await db.execute(sql.raw(`
      CREATE TRIGGER test_task_session_trigger
      BEFORE INSERT OR UPDATE ON agent_task_sessions
      FOR EACH ROW EXECUTE FUNCTION test_task_session_trigger_fn();
    `));
  }

  it("never exposes a terminal run before its resumable task session", async () => {
    const ids = await seed();
    const runId = await insertRun(ids);
    await installSessionTrigger("PERFORM pg_sleep(0.5);");

    const finalization = finalizeRunningRunWithTaskSession(db, {
      runId,
      status: "succeeded",
      patch: { finishedAt: new Date() },
      taskSessionMutation: upsert(ids, "session-atomic"),
    });
    await new Promise((resolve) => setTimeout(resolve, 150));

    // Mid-transaction an observer still sees the run as running and no session.
    expect(await runStatus(runId)).toBe("running");
    expect(await sessionRow(ids.taskKey)).toBeNull();

    const result = await finalization;
    expect(result.updated).toBe(true);
    expect(result.run?.status).toBe("succeeded");
    expect(await runStatus(runId)).toBe("succeeded");
    expect(await sessionRow(ids.taskKey)).toMatchObject({
      sessionDisplayId: "session-atomic",
      lastRunId: runId,
      lastError: null,
    });
  }, 20_000);

  it("clears an invalid session atomically as a tombstone", async () => {
    const ids = await seed();
    const runId = await insertRun(ids, { startedAt: new Date("2026-07-21T09:00:00.000Z") });
    await db.insert(agentTaskSessions).values({
      companyId: ids.companyId,
      agentId: ids.agentId,
      adapterType: ADAPTER,
      taskKey: ids.taskKey,
      sessionParamsJson: { sessionId: "session-to-clear" },
      sessionDisplayId: "session-to-clear",
      lastRunId: null,
    });
    await installSessionTrigger("PERFORM pg_sleep(0.5);");

    const finalization = finalizeRunningRunWithTaskSession(db, {
      runId,
      status: "succeeded",
      patch: { finishedAt: new Date() },
      taskSessionMutation: clear(ids),
    });
    await new Promise((resolve) => setTimeout(resolve, 150));
    expect(await runStatus(runId)).toBe("running");
    expect((await sessionRow(ids.taskKey))?.sessionDisplayId).toBe("session-to-clear");

    expect((await finalization).updated).toBe(true);
    const tombstone = await sessionRow(ids.taskKey);
    // The tombstone keeps the clearing run so it orders by (start, id).
    expect(tombstone).toMatchObject({ sessionParamsJson: null, sessionDisplayId: null, lastRunId: runId });
  }, 20_000);

  it("rolls back the terminal status when the session write fails", async () => {
    const ids = await seed();
    const runId = await insertRun(ids);
    await installSessionTrigger("RAISE EXCEPTION 'synthetic session persistence failure';");

    const error = await finalizeRunningRunWithTaskSession(db, {
      runId,
      status: "succeeded",
      patch: { finishedAt: new Date() },
      taskSessionMutation: upsert(ids, "never-persisted"),
    }).then(() => null, (err: unknown) => err as Error & { cause?: Error });
    expect(`${error?.message} ${error?.cause?.message}`).toMatch(/synthetic session persistence failure/);

    // Neither half committed: the run can still be finalized by the failure path.
    expect(await runStatus(runId)).toBe("running");
    expect(await sessionRow(ids.taskKey)).toBeNull();

    await db.execute(sql.raw("DROP TRIGGER test_task_session_trigger ON agent_task_sessions;"));
    const failed = await finalizeRunningRunWithTaskSession(db, {
      runId,
      status: "failed",
      patch: { finishedAt: new Date(), error: "session persistence failed" },
      taskSessionMutation: null,
    });
    expect(failed.updated).toBe(true);
    expect(await runStatus(runId)).toBe("failed");
  }, 20_000);

  it("does not write a session for a stale finalizer that lost the status compare-and-set", async () => {
    const ids = await seed();
    const runId = await insertRun(ids, { status: "cancelled" });
    await db.insert(agentTaskSessions).values({
      companyId: ids.companyId,
      agentId: ids.agentId,
      adapterType: ADAPTER,
      taskKey: ids.taskKey,
      sessionParamsJson: { sessionId: "session-kept" },
      sessionDisplayId: "session-kept",
      lastRunId: runId,
    });

    const result = await finalizeRunningRunWithTaskSession(db, {
      runId,
      status: "succeeded",
      patch: { finishedAt: new Date() },
      taskSessionMutation: upsert(ids, "late-session"),
    });

    expect(result.updated).toBe(false);
    expect(result.run?.status).toBe("cancelled");
    expect((await sessionRow(ids.taskKey))?.sessionDisplayId).toBe("session-kept");
  });

  it("keeps a finalized run and its session when cancellation arrives late", async () => {
    const ids = await seed();
    const runId = await insertRun(ids);
    const finalized = await finalizeRunningRunWithTaskSession(db, {
      runId,
      status: "succeeded",
      patch: { finishedAt: new Date() },
      taskSessionMutation: upsert(ids, "session-finalized"),
    });
    expect(finalized.updated).toBe(true);

    // What cancelRun now does after process termination.
    const cancel = await finalizeRunningRunWithTaskSession(db, {
      runId,
      status: "cancelled",
      patch: { finishedAt: new Date(), error: "late cancel" },
      expectedStatuses: CANCELLABLE,
    });
    expect(cancel.updated).toBe(false);
    expect(await runStatus(runId)).toBe("succeeded");
    expect((await sessionRow(ids.taskKey))?.sessionDisplayId).toBe("session-finalized");
  });

  it("does not let an older overlapping run overwrite a newer run's session", async () => {
    const ids = await seed();
    const olderRunId = await insertRun(ids, { startedAt: new Date("2026-07-21T10:00:00.000Z") });
    const newerRunId = await insertRun(ids, { startedAt: new Date("2026-07-21T10:01:00.000Z") });

    await finalizeRunningRunWithTaskSession(db, {
      runId: newerRunId,
      status: "succeeded",
      taskSessionMutation: upsert(ids, "session-newer"),
    });
    const older = await finalizeRunningRunWithTaskSession(db, {
      runId: olderRunId,
      status: "succeeded",
      taskSessionMutation: upsert(ids, "session-older"),
    });

    expect(older.updated).toBe(true);
    expect(await sessionRow(ids.taskKey)).toMatchObject({
      sessionDisplayId: "session-newer",
      lastRunId: newerRunId,
    });
  });

  it("keeps a newer run's clear authoritative over an older in-flight first insert", async () => {
    const ids = await seed();
    const olderRunId = await insertRun(ids, { startedAt: new Date("2026-07-21T11:00:00.000Z") });
    const newerRunId = await insertRun(ids, { startedAt: new Date("2026-07-21T11:01:00.000Z") });
    await installSessionTrigger(`
      IF NEW.session_display_id = 'stale-older' THEN
        PERFORM pg_sleep(0.5);
      END IF;
    `);

    const olderFinalization = finalizeRunningRunWithTaskSession(db, {
      runId: olderRunId,
      status: "succeeded",
      taskSessionMutation: upsert(ids, "stale-older"),
    });
    await new Promise((resolve) => setTimeout(resolve, 150));
    const newerClear = await finalizeRunningRunWithTaskSession(db, {
      runId: newerRunId,
      status: "succeeded",
      taskSessionMutation: clear(ids),
    });
    expect((await olderFinalization).updated).toBe(true);
    expect(newerClear.updated).toBe(true);

    const tombstone = await sessionRow(ids.taskKey);
    expect(tombstone).toMatchObject({ sessionParamsJson: null, sessionDisplayId: null, lastRunId: newerRunId });
  }, 20_000);

  it("lets a newer run replace an older run's session", async () => {
    const ids = await seed();
    const olderRunId = await insertRun(ids, { startedAt: new Date("2026-07-21T12:00:00.000Z") });
    const newerRunId = await insertRun(ids, { startedAt: new Date("2026-07-21T12:01:00.000Z") });
    await finalizeRunningRunWithTaskSession(db, {
      runId: olderRunId,
      status: "succeeded",
      taskSessionMutation: upsert(ids, "session-old"),
    });
    await finalizeRunningRunWithTaskSession(db, {
      runId: newerRunId,
      status: "failed",
      taskSessionMutation: { ...upsert(ids, "session-new"), lastError: "boom" } as FinalizeRunTaskSessionMutation,
    });
    expect(await sessionRow(ids.taskKey)).toMatchObject({
      sessionDisplayId: "session-new",
      lastRunId: newerRunId,
      lastError: "boom",
    });
  });

  describe("runs with equal start times", () => {
    const startedAt = new Date("2026-09-25T12:00:00.123Z");
    const firstRunId = "00000000-0000-4000-8000-000000000001";
    const secondRunId = "00000000-0000-4000-8000-000000000002";

    it("keeps a clear by the later run authoritative over the earlier run's save", async () => {
      const ids = await seed();
      await insertRun(ids, { id: firstRunId, startedAt });
      await insertRun(ids, { id: secondRunId, startedAt });

      await finalizeRunningRunWithTaskSession(db, { runId: secondRunId, status: "succeeded", taskSessionMutation: clear(ids) });
      const stale = await finalizeRunningRunWithTaskSession(db, {
        runId: firstRunId,
        status: "succeeded",
        taskSessionMutation: upsert(ids, "old-session"),
      });

      expect(stale.updated).toBe(true);
      expect(await sessionRow(ids.taskKey)).toMatchObject({
        sessionParamsJson: null,
        sessionDisplayId: null,
        lastRunId: secondRunId,
      });
    });

    it("lets the later run save after the earlier run's clear", async () => {
      const ids = await seed();
      await insertRun(ids, { id: firstRunId, startedAt });
      await insertRun(ids, { id: secondRunId, startedAt });

      await finalizeRunningRunWithTaskSession(db, { runId: firstRunId, status: "succeeded", taskSessionMutation: clear(ids) });
      await finalizeRunningRunWithTaskSession(db, {
        runId: secondRunId,
        status: "succeeded",
        taskSessionMutation: upsert(ids, "new-session"),
      });

      expect(await sessionRow(ids.taskKey)).toMatchObject({ sessionDisplayId: "new-session", lastRunId: secondRunId });
    });

    it("blocks a run that started exactly at an explicit reset, not one that started after it", async () => {
      const ids = await seed();
      await db.insert(agentTaskSessions).values({
        companyId: ids.companyId,
        agentId: ids.agentId,
        adapterType: ADAPTER,
        taskKey: ids.taskKey,
        sessionParamsJson: null,
        sessionDisplayId: null,
        lastRunId: null,
        updatedAt: startedAt,
      });
      const tiedRunId = await insertRun(ids, { startedAt });
      await finalizeRunningRunWithTaskSession(db, {
        runId: tiedRunId,
        status: "succeeded",
        taskSessionMutation: upsert(ids, "pre-reset-session"),
      });
      expect(await sessionRow(ids.taskKey)).toMatchObject({ sessionParamsJson: null, sessionDisplayId: null });

      const laterRunId = await insertRun(ids, { startedAt: new Date(startedAt.getTime() + 1) });
      await finalizeRunningRunWithTaskSession(db, {
        runId: laterRunId,
        status: "succeeded",
        taskSessionMutation: upsert(ids, "post-reset-session"),
      });
      expect(await sessionRow(ids.taskKey)).toMatchObject({ sessionDisplayId: "post-reset-session", lastRunId: laterRunId });
    });
  });

  describe("explicit session reset", () => {
    it("blocks a run that started before the reset from resurrecting the session", async () => {
      const ids = await seed();
      const heartbeat = heartbeatService(db);
      const priorRunId = await insertRun(ids, { status: "succeeded", startedAt: new Date(Date.now() - 60_000) });
      await db.insert(agentTaskSessions).values({
        companyId: ids.companyId,
        agentId: ids.agentId,
        adapterType: ADAPTER,
        taskKey: ids.taskKey,
        sessionParamsJson: { sessionId: "session-before-reset" },
        sessionDisplayId: "session-before-reset",
        lastRunId: priorRunId,
      });
      const inFlightRunId = await insertRun(ids, { startedAt: new Date(Date.now() - 1_000) });

      const reset = await heartbeat.resetRuntimeSession(ids.agentId, { taskKey: ids.taskKey });
      expect(reset?.clearedTaskSessions).toBe(1);
      expect(await heartbeat.listTaskSessions(ids.agentId)).toHaveLength(0);

      const late = await finalizeRunningRunWithTaskSession(db, {
        runId: inFlightRunId,
        status: "succeeded",
        taskSessionMutation: upsert(ids, "session-before-reset"),
      });
      expect(late.updated).toBe(true);
      expect(await sessionRow(ids.taskKey)).toMatchObject({ sessionParamsJson: null, sessionDisplayId: null });
      expect(await heartbeat.listTaskSessions(ids.agentId)).toHaveLength(0);

      // A run that starts after the reset checkpoints normally.
      const freshRunId = await insertRun(ids, { startedAt: new Date(Date.now() + 1_000) });
      await finalizeRunningRunWithTaskSession(db, {
        runId: freshRunId,
        status: "succeeded",
        taskSessionMutation: upsert(ids, "session-after-reset"),
      });
      expect(await sessionRow(ids.taskKey)).toMatchObject({
        sessionDisplayId: "session-after-reset",
        lastRunId: freshRunId,
      });
      expect(await heartbeat.listTaskSessions(ids.agentId)).toHaveLength(1);
    });

    it("covers a task whose first run has not checkpointed yet", async () => {
      const ids = await seed();
      const heartbeat = heartbeatService(db);
      const inFlightRunId = await insertRun(ids, { startedAt: new Date(Date.now() - 1_000) });

      const reset = await heartbeat.resetRuntimeSession(ids.agentId, { taskKey: ids.taskKey });
      expect(reset?.clearedTaskSessions).toBe(0);

      await finalizeRunningRunWithTaskSession(db, {
        runId: inFlightRunId,
        status: "succeeded",
        taskSessionMutation: upsert(ids, "pre-reset-first-session"),
      });
      expect(await heartbeat.listTaskSessions(ids.agentId)).toHaveLength(0);
    });

    it("tombstones every task session on a full agent reset", async () => {
      const ids = await seed();
      const heartbeat = heartbeatService(db);
      const priorRunId = await insertRun(ids, { status: "succeeded", startedAt: new Date(Date.now() - 60_000) });
      const otherTaskKey = randomUUID();
      await db.insert(agentTaskSessions).values([ids.taskKey, otherTaskKey].map((taskKey) => ({
        companyId: ids.companyId,
        agentId: ids.agentId,
        adapterType: ADAPTER,
        taskKey,
        sessionParamsJson: { sessionId: `session-${taskKey}` },
        sessionDisplayId: `session-${taskKey}`,
        lastRunId: priorRunId,
      })));
      const inFlightRunId = await insertRun(ids, { startedAt: new Date(Date.now() - 1_000) });

      const reset = await heartbeat.resetRuntimeSession(ids.agentId);
      expect(reset?.clearedTaskSessions).toBe(2);
      await finalizeRunningRunWithTaskSession(db, {
        runId: inFlightRunId,
        status: "succeeded",
        taskSessionMutation: upsert(ids, `session-${ids.taskKey}`),
      });
      expect(await heartbeat.listTaskSessions(ids.agentId)).toHaveLength(0);
      const runtime = await heartbeat.getRuntimeState(ids.agentId);
      expect(runtime?.sessionParamsJson).toBeNull();
    });
  });
});
