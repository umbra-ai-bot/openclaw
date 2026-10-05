import { ErrorCodes, errorShape } from "../../../packages/gateway-protocol/src/index.js";
// Session quota follows the prepared execution runtime, not a same-email saved login.
import { resolveAgentDir, resolveAgentWorkspaceDir } from "../../agents/agent-scope.js";
import { loadProviderUsageSummary } from "../../infra/provider-usage.load.js";
import { buildCodexSyntheticUsageAuth } from "../../status/codex-synthetic-usage.js";
import { resolveRequestedSessionAgentId } from "../session-request-agent.js";
import { withReadySessionRows } from "../session-row-prepared-read.js";
import { prepareProjectedSessionPresentation } from "../session-row-presentation.js";
import { getSessionRowProjection } from "../session-row-projection-access.js";
import { hiddenSessionNotFound } from "../session-sharing-policy.js";
import { resolveSessionStoreIdentity } from "../session-store-key.js";
import type { GatewayRequestHandlerOptions } from "./types.js";

export async function handleSessionProviderUsage(
  options: GatewayRequestHandlerOptions,
  key: string,
  agentId?: string,
): Promise<void> {
  const { context, client, respond, signal, hasCurrentClientAuthority } = options;
  const config = context.getRuntimeConfig();
  const requested = resolveRequestedSessionAgentId(config, key, agentId);
  if (!requested.ok) {
    respond(false, undefined, requested.error);
    return;
  }
  const projection = getSessionRowProjection(context);
  if (!projection) {
    respond(false, undefined, errorShape(ErrorCodes.UNAVAILABLE, "Session rows are initializing."));
    return;
  }
  const identity = resolveSessionStoreIdentity({
    cfg: config,
    sessionKey: key,
    agentId: requested.agentId,
  });
  const query = { key: identity.canonicalKey, agentId: identity.agentId };
  const queries = () => [query];
  const executionBinding = (
    row: ReturnType<import("../session-row-prepared-read.js").SessionRowReadView["present"]>,
    profileId?: string,
  ) =>
    JSON.stringify([
      row.agentRuntime?.id,
      row.activeModelProvider ?? row.modelProvider,
      row.activeModel ?? row.model,
      profileId,
    ]);
  const selected = await withReadySessionRows(projection, queries, (read) => {
    const record = read.describe(query);
    const sharing = prepareProjectedSessionPresentation(read, client).sharing;
    if (!record || sharing.entryFilter?.(record.key, record.entry) === false) {
      return undefined;
    }
    const row = read.present(record);
    return {
      record,
      row,
      config: read.state.cfg,
      binding: executionBinding(row, record.entry.authProfileOverride),
    };
  });
  if (!selected) {
    respond(false, undefined, hiddenSessionNotFound(identity.canonicalKey));
    return;
  }
  signal?.throwIfAborted();
  const provider = selected.row.activeModelProvider ?? selected.row.modelProvider;
  const nativeCodex = selected.row.agentRuntime?.id === "codex" && provider === "openai";
  // This selector supplies execution quota only. Other runtimes retain the generic
  // auth-status path; an empty result is not permission to invent a zero count.
  const summary = nativeCodex
    ? await loadProviderUsageSummary({
        config: selected.config,
        agentDir: resolveAgentDir(selected.config, identity.agentId),
        workspaceDir: resolveAgentWorkspaceDir(selected.config, identity.agentId),
        providers: ["openai"],
        auth: [
          buildCodexSyntheticUsageAuth({
            authProfileId: selected.record.entry.authProfileOverride,
          }),
        ],
        timeoutMs: 8_000,
      })
    : { updatedAt: Date.now(), providers: [] };
  await withReadySessionRows(projection, queries, (read) => {
    const record = read.describe(query, selected.record);
    const sharing = prepareProjectedSessionPresentation(read, client).sharing;
    const current =
      record &&
      read.state.cfg === selected.config &&
      context.getRuntimeConfig() === selected.config &&
      executionBinding(read.present(record), record.entry.authProfileOverride) ===
        selected.binding &&
      sharing.entryFilter?.(record.key, record.entry) !== false;
    if (!current || signal?.aborted || hasCurrentClientAuthority?.() === false) {
      respond(
        false,
        undefined,
        errorShape(ErrorCodes.UNAVAILABLE, "Session changed while reading usage."),
      );
      return;
    }
    // Authorization and delivery share this prepared owner's synchronous frame.
    respond(true, summary, undefined);
  });
}
