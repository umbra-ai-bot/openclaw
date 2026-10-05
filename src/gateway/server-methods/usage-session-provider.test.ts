// Selected-session quota must not borrow a same-email saved account's allowance.
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { SessionEntry } from "../../config/sessions/types.js";
import type { GatewayRequestHandlerOptions } from "./types.js";

const fixture = vi.hoisted(() => ({
  visible: true,
  current: true,
  runtime: "codex",
  entry: { sessionId: "session", updatedAt: 1 } as SessionEntry,
  config: { agents: { list: [{ id: "main", default: true }, { id: "other" }] } },
  load: vi.fn(),
}));
vi.mock("../../infra/provider-usage.load.js", () => ({ loadProviderUsageSummary: fixture.load }));
vi.mock("../session-row-projection-access.js", () => ({ getSessionRowProjection: () => ({}) }));
vi.mock("../session-row-presentation.js", () => ({
  prepareProjectedSessionPresentation: () => ({ sharing: { entryFilter: () => fixture.visible } }),
}));
vi.mock("../session-row-prepared-read.js", () => ({
  withReadySessionRows: async (
    _owner: unknown,
    _queries: unknown,
    consume: (read: unknown) => unknown,
  ) =>
    consume({
      state: { cfg: fixture.config },
      describe: (_query: unknown, captured?: unknown) =>
        captured && !fixture.current
          ? undefined
          : {
              key: "agent:main:main",
              agentId: "main",
              entry: fixture.entry,
            },
      present: () => ({ modelProvider: "openai", agentRuntime: { id: fixture.runtime } }),
    }),
}));

import { usageHandlers } from "./usage.js";

describe("session provider quota", () => {
  beforeEach(() => {
    fixture.visible = true;
    fixture.current = true;
    fixture.runtime = "codex";
    fixture.entry = { sessionId: "session", updatedAt: 1 };
    fixture.load.mockReset().mockResolvedValue({
      updatedAt: 1,
      providers: [
        {
          provider: "openai",
          displayName: "OpenAI",
          plan: "pro",
          windows: [{ label: "Week", usedPercent: 29 }],
        },
      ],
    });
  });
  async function request(key = "agent:main:main", agentId = "main") {
    const respond = vi.fn();
    await usageHandlers["usage.status"]!({
      context: { getRuntimeConfig: () => fixture.config },
      client: null,
      respond,
      params: { key, agentId },
    } as unknown as GatewayRequestHandlerOptions);
    return respond;
  }
  it.each([undefined, "openai:pinned"])(
    "uses native quota with only the explicit session pin: %s",
    async (profileId) => {
      fixture.entry.authProfileOverride = profileId;
      const respond = await request();
      expect(fixture.load).toHaveBeenCalledWith(
        expect.objectContaining({
          providers: ["openai"],
          auth: [
            expect.objectContaining({
              hookProvider: "codex",
              ...(profileId ? { authProfileId: profileId } : {}),
            }),
          ],
        }),
      );
      if (!profileId) {
        expect(fixture.load.mock.calls[0][0].auth[0]).not.toHaveProperty("authProfileId");
      }
      expect(respond).toHaveBeenCalledWith(
        true,
        expect.objectContaining({ providers: [expect.objectContaining({ plan: "pro" })] }),
        undefined,
      );
    },
  );
  it("rejects another agent's session before querying provider usage", async () => {
    const respond = await request("agent:other:private", "main");
    expect(respond.mock.calls[0][0]).toBe(false);
    expect(fixture.load).not.toHaveBeenCalled();
  });
  it("preserves an API-key pin's unavailable quota without querying another subscription", async () => {
    fixture.entry.authProfileOverride = "openai:api-key";
    const unavailable = {
      updatedAt: 1,
      providers: [
        {
          provider: "openai",
          displayName: "OpenAI",
          windows: [],
          error: "API-key quota unavailable",
        },
      ],
    };
    fixture.load.mockResolvedValue(unavailable);
    const respond = await request();
    expect(fixture.load).toHaveBeenCalledOnce();
    expect(fixture.load.mock.calls[0][0].auth).toEqual([
      expect.objectContaining({ hookProvider: "codex", authProfileId: "openai:api-key" }),
    ]);
    expect(respond).toHaveBeenCalledWith(true, unavailable, undefined);
  });
  it("rejects a hidden session", async () => {
    fixture.visible = false;
    const respond = await request();
    expect(respond.mock.calls[0][0]).toBe(false);
    expect(fixture.load).not.toHaveBeenCalled();
  });
  it("discards a result after its session owner is replaced", async () => {
    fixture.load.mockImplementation(async () => {
      fixture.current = false;
      return { updatedAt: 1, providers: [] };
    });
    const respond = await request();
    expect(respond.mock.calls[0][0]).toBe(false);
  });
  it.each(["pin", "runtime"])(
    "discards quota when its %s changes during the read",
    async (change) => {
      fixture.load.mockImplementation(async () => {
        if (change === "pin") {
          fixture.entry.authProfileOverride = "openai:other";
        } else {
          fixture.runtime = "openclaw";
        }
        return { updatedAt: 1, providers: [] };
      });
      const respond = await request();
      expect(respond.mock.calls[0][0]).toBe(false);
    },
  );
  it("does not turn another runtime into a native subscription query", async () => {
    fixture.runtime = "openclaw";
    const respond = await request();
    expect(fixture.load).not.toHaveBeenCalled();
    expect(respond).toHaveBeenCalledWith(
      true,
      expect.objectContaining({ providers: [] }),
      undefined,
    );
  });
});
