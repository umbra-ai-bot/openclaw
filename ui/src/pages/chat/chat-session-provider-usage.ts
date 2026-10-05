import type { UsageSummary } from "../../../../src/infra/provider-usage.types.js";
import type { GatewaySessionRow } from "../../api/types.ts";
import { formatUiError } from "../../lib/format-error.ts";
import { loadModelAuthStatus } from "../../lib/model-auth.ts";
import type { ProviderUsageDisplayProps } from "../../lib/provider-quota-summary.ts";
import { captureChatModelSelectionAuthority } from "./chat-session.ts";
import type { ChatPageHost } from "./chat-state-host.ts";
import { resolveChatAgentId } from "./chat-state-route.ts";

export function readChatSessionProviderUsage(host: ChatPageHost): UsageSummary | null {
  const usage = host.chatSessionUsage;
  return host.connected &&
    usage?.client === host.client &&
    usage.connectionEpoch === host.connectionEpoch &&
    usage.sessionKey === host.sessionKey &&
    usage.agentId === resolveChatAgentId(host) &&
    usage.ownsSelection()
    ? usage.summary
    : null;
}

export async function refreshChatModelAuthStatus(host: ChatPageHost, opts?: { refresh?: boolean }) {
  if (!host.client || !host.connected) {
    return;
  }
  const client = host.client;
  const connectionEpoch = host.connectionEpoch;
  const agentId = resolveChatAgentId(host);
  const sessionKey = host.sessionKey;
  const ownsSelection = captureChatModelSelectionAuthority(host);
  const requestVersion = ++host.modelAuthStatusRequestVersion;
  const ownsRequest = () =>
    host.client === client &&
    host.connected &&
    host.connectionEpoch === connectionEpoch &&
    host.modelAuthStatusRequestVersion === requestVersion &&
    ownsSelection() &&
    resolveChatAgentId(host) === agentId;
  host.chatSessionUsage = {
    sessionKey,
    agentId,
    client,
    connectionEpoch,
    ownsSelection,
    summary: null,
  };
  const sessionUsage = client
    .request<UsageSummary>("usage.status", { key: sessionKey, agentId })
    .then(
      (summary) => {
        if (ownsRequest() && host.sessionKey === sessionKey) {
          host.chatSessionUsage = {
            sessionKey,
            agentId,
            client,
            connectionEpoch,
            ownsSelection,
            summary,
          };
          host.requestUpdate?.();
        }
      },
      () => {
        // The current native quota remains unavailable, never replaced by another login's quota.
      },
    );
  try {
    const result = await loadModelAuthStatus(client, {
      ...opts,
      agentId,
    });
    if (!ownsRequest()) {
      return;
    }
    host.modelAuthStatusResult = result;
    host.modelAuthStatusError = result.unavailable?.message ?? null;
  } catch (err) {
    if (!ownsRequest()) {
      return;
    }
    host.modelAuthStatusResult = { ts: 0, providers: [] };
    host.modelAuthStatusError = formatUiError(err);
  }
  await sessionUsage;
}

export function readChatComposerProviderUsage(
  host: ChatPageHost,
  selectedSession: GatewaySessionRow | undefined,
): ProviderUsageDisplayProps {
  return {
    basePath: host.basePath,
    modelAuthStatusResult: host.modelAuthStatusResult,
    ...(selectedSession?.agentRuntime?.id === "codex" &&
    (selectedSession.activeModelProvider ?? selectedSession.modelProvider) === "openai"
      ? { sessionUsage: readChatSessionProviderUsage(host) }
      : {}),
  };
}
