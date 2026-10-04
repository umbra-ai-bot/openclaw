import {
  runSessionStartupMigration,
  type SessionStartupMigrationLogger,
} from "../config/sessions/startup-migration.js";
import type { OpenClawConfig } from "../config/types.openclaw.js";
import { hasActiveGatewayStateOwner } from "../infra/gateway-state-owner.js";
import {
  assertExistingDatabaseIdentity,
  readDatabasePathIdentitySync,
} from "../infra/sqlite-worker-identity.js";
import {
  isSubagentSessionKey,
  isIncognitoSessionKey,
  resolveAgentIdFromSessionKey,
} from "../routing/session-key.js";
import { captureAgentDatabaseAdmission } from "../state/agent-database-admission.js";
import { AGENT_DATABASE_PREFLIGHT_CONCURRENCY } from "../state/openclaw-agent-db-contract.js";
import type { OpenClawAgentDatabaseOptions } from "../state/openclaw-agent-db-contract.js";
import { resolveOpenClawAgentSqlitePath } from "../state/openclaw-agent-db.paths.js";
import { resolveOpenClawStateSqlitePath } from "../state/openclaw-state-db.paths.js";
import { captureOpenClawStateReadContext } from "../state/openclaw-state-worker-context.js";
import { runTasksWithConcurrency } from "../utils/run-with-concurrency.js";
import { measureStartup, type GatewayStartupTrace } from "./server-startup-trace.js";

type SessionMigrationDeps = Parameters<typeof runSessionStartupMigration>[0]["deps"] & {
  reconcileSessionTranscriptIndexes?: typeof import("../config/sessions/session-transcript-reconcile.js").reconcileSessionTranscriptIndexes;
};

async function reconcileStartupOrphans(
  database: OpenClawAgentDatabaseOptions & { path: string },
  log: SessionStartupMigrationLogger,
  assertCurrent?: () => void,
) {
  const env = database.env ?? process.env;
  const statePath = resolveOpenClawStateSqlitePath(env);
  if (!hasActiveGatewayStateOwner(statePath)) {
    return undefined;
  }
  const [
    { hasLiveSubagentSessionRecoveryOwner },
    { isStartupSessionInterruptionCandidate },
    { hasSessionEntriesByStatusReadOnly, listSessionEntriesByStatus },
    { readActiveGatewayLockIdentity },
    { isSessionWorkAdmissionActive },
    { recordGatewaySessionRunFailure },
  ] = await Promise.all([
    import("../agents/subagents/registry/subagent-session-reconciliation.js"),
    import("../config/sessions/session-accessor.sqlite-transcript-reports.types.js"),
    import("../config/sessions/session-entry-status-read.js"),
    import("../infra/gateway-lock.js"),
    import("../sessions/session-lifecycle-admission.js"),
    import("../sessions/session-run-error.js"),
  ]);
  assertCurrent?.();
  try {
    const running = await hasSessionEntriesByStatusReadOnly(
      { ...database, storePath: database.path },
      ["running"],
    );
    assertCurrent?.();
    if (!running) {
      return undefined;
    }
  } catch {
    // The writable owner retains schema repair and integrity diagnosis for uncertain reads.
  }
  const lock = await readActiveGatewayLockIdentity({ env, requireInspection: true });
  if (lock?.pid !== process.pid || !lock.ownerId) {
    return undefined;
  }
  const assertGatewayOwner = () => {
    assertCurrent?.();
    if (!hasActiveGatewayStateOwner(statePath)) {
      throw new Error("startup Gateway ownership changed or could not be verified");
    }
  };
  assertGatewayOwner();
  const selected = await listSessionEntriesByStatus({ ...database, storePath: database.path }, [
    "running",
  ]);
  assertGatewayOwner();
  let interrupted = 0;
  let retained = 0;
  for (const { entry, sessionKey } of selected) {
    if (
      !isSubagentSessionKey(sessionKey) ||
      isIncognitoSessionKey(sessionKey) ||
      !isStartupSessionInterruptionCandidate(entry, performance.timeOrigin)
    ) {
      continue;
    }
    const identity = { sessionKey, sessionId: entry.sessionId };
    const target = {
      agentId: resolveAgentIdFromSessionKey(sessionKey),
      env,
      sessionKey,
      storePath: database.path,
    };
    const hasOwner = () =>
      hasLiveSubagentSessionRecoveryOwner(identity) ||
      isSessionWorkAdmissionActive(database.path, [sessionKey, entry.sessionId]);
    const assertOwnerless = () => {
      assertGatewayOwner();
      if (hasOwner()) {
        throw new Error("a current or retained run/task owns this session");
      }
    };
    try {
      assertGatewayOwner();
      // Retained runs belong to registry recovery; only session-only orphans settle here.
      if (hasOwner()) {
        retained++;
        continue;
      }
      // This is the repair observation, not a reconstructed execution finish time.
      const endedAt = Date.now();
      const error = "subagent run was interrupted before a terminal lifecycle event was persisted";
      const outcome = await recordGatewaySessionRunFailure({
        target: {
          ...target,
          sessionId: entry.sessionId,
          expectedLifecycleRevision: entry.lifecycleRevision,
        },
        // A recovery-only receipt identity must not suppress the notice after partial output.
        runId: `startup-orphan:${entry.sessionId}:${entry.lifecycleRunId ?? entry.startedAt}`,
        error,
        assertCommitAllowed: assertOwnerless,
        startupInterruption: {
          expected: {
            sessionId: entry.sessionId,
            lifecycleRevision: entry.lifecycleRevision,
            lifecycleRunId: entry.lifecycleRunId,
            startedAt: entry.startedAt,
            updatedAt: entry.updatedAt,
          },
          processStartedAt: performance.timeOrigin,
          endedAt,
          gatewayOwner: { pid: process.pid, owner: lock.ownerId },
        },
      });
      if (outcome === "retained") {
        retained++;
      } else {
        interrupted++;
      }
    } catch (error) {
      log.warn(`session: retained startup subagent ${sessionKey}: ${String(error)}`);
    }
  }
  return { interrupted, retained };
}

export type PreparedStartupSessionDatabase = {
  database: OpenClawAgentDatabaseOptions & { path: string };
  assertCurrent: () => void;
};

/** Admit each physical store once; repair consumes these targets after readiness. */
export async function prepareGatewayStartupSessions(params: {
  cfg: OpenClawConfig;
  env?: NodeJS.ProcessEnv;
  agentIds?: ReadonlySet<string>;
  assertCurrent?: () => void;
  log: SessionStartupMigrationLogger;
  deps?: SessionMigrationDeps;
}): Promise<PreparedStartupSessionDatabase[]> {
  const databases: PreparedStartupSessionDatabase[] = [];
  await runSessionStartupMigration({
    ...params,
    handoffDatabase: async (database) => {
      params.assertCurrent?.();
      const identity = readDatabasePathIdentitySync(resolveOpenClawAgentSqlitePath(database));
      const state = captureOpenClawStateReadContext(resolveOpenClawStateSqlitePath(database.env));
      const assertAdmitted = captureAgentDatabaseAdmission(database.agentId, { env: database.env });
      databases.push({
        database: { ...database, path: identity.canonicalPath },
        assertCurrent: () => {
          state.admission.assertCurrent();
          assertAdmitted();
          assertExistingDatabaseIdentity(identity.canonicalPath, identity.key, identity.birthtime);
        },
      });
    },
  });
  return databases;
}

/** Repair admitted startup targets without rediscovering stores or repeating admission. */
export async function runGatewaySessionStartupMaintenance(params: {
  databases: readonly PreparedStartupSessionDatabase[];
  assertCurrent?: () => void;
  signal?: AbortSignal;
  log: SessionStartupMigrationLogger;
  deps?: Pick<SessionMigrationDeps, "reconcileSessionTranscriptIndexes">;
  startupTrace?: GatewayStartupTrace;
}): Promise<void> {
  let reconcile = params.deps?.reconcileSessionTranscriptIndexes;
  let reconciledSessions = 0;
  let interruptedSubagents = 0;
  let retainedSubagents = 0;
  const outcome = await runTasksWithConcurrency({
    limit: AGENT_DATABASE_PREFLIGHT_CONCURRENCY,
    errorMode: "stop",
    tasks: params.databases.map(
      ({ database, assertCurrent: assertDatabaseCurrent }) =>
        async () => {
          const assertCurrent = () => {
            params.signal?.throwIfAborted();
            params.assertCurrent?.();
            assertDatabaseCurrent();
          };
          assertCurrent();
          try {
            const result = await measureStartup(
              params.startupTrace,
              "startup.maintenance.session-orphans",
              () => reconcileStartupOrphans(database, params.log, assertCurrent),
            );
            interruptedSubagents += result?.interrupted ?? 0;
            retainedSubagents += result?.retained ?? 0;
          } catch (error) {
            assertCurrent();
            params.log.warn(
              `session: retained startup orphans because ownership could not be verified: ${String(error)}`,
            );
          }
          const result = await measureStartup(
            params.startupTrace,
            "startup.maintenance.session-transcripts",
            async () => {
              reconcile ??= (await import("../config/sessions/session-transcript-reconcile.js"))
                .reconcileSessionTranscriptIndexes;
              assertCurrent();
              return reconcile({ ...database, assertCurrent, signal: params.signal });
            },
          );
          assertCurrent();
          reconciledSessions += result.reconciledSessions;
        },
    ),
  });
  if (outcome.hasError) {
    throw outcome.firstError;
  }
  if (interruptedSubagents > 0 || retainedSubagents > 0) {
    params.log.info(
      `session: startup subagents: ${interruptedSubagents} interrupted, ${retainedSubagents} retained by run/task owners`,
    );
  }
  if (reconciledSessions > 0) {
    params.log.info(`session: rebuilt ${reconciledSessions} transcript projection(s)`);
  }
}
