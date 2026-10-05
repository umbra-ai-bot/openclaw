// @vitest-environment node
import { afterEach, describe, expect, it, vi } from "vitest";
import { updateChatRunProgressSnapshot } from "../../../../src/gateway/server-chat-progress-snapshot.js";
import { resetToolStream } from "./tool-stream-state.ts";
import { createHost } from "./tool-stream.test-helpers.ts";
import { handleAgentEvent } from "./tool-stream.ts";

type AgentEvent = NonNullable<Parameters<typeof handleAgentEvent>[1]>;

afterEach(() => {
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

function agentEvent(
  runId: string,
  seq: number,
  stream: AgentEvent["stream"],
  data: AgentEvent["data"],
  sessionKey?: string,
): AgentEvent {
  return {
    runId,
    seq,
    stream,
    ts: Date.now(),
    ...(sessionKey ? { sessionKey } : {}),
    data,
  };
}

describe("app-tool-stream run usage", () => {
  it.each(["full", "summary"] as const)(
    "keeps %s Gateway recovery context unavailable after compaction",
    (mode) => {
      const context = agentEvent(
        "native-run",
        1,
        "usage",
        { activeContextTokens: 200_000, modelContextWindow: 258_400, outputTokens: 50 },
        "main",
      );
      let snapshot = updateChatRunProgressSnapshot(undefined, context, mode);
      snapshot = updateChatRunProgressSnapshot(
        snapshot,
        agentEvent("native-run", 2, "compaction", { phase: "start" }, "main"),
        mode,
      );
      snapshot = updateChatRunProgressSnapshot(
        snapshot,
        agentEvent("native-run", 3, "usage", { outputTokens: 60 }, "main"),
        mode,
      );
      const host = createHost({ chatRunId: "native-run" });
      for (const event of snapshot?.events ?? []) {
        handleAgentEvent(host, event);
      }
      expect(host.chatRunUsageById?.get("native-run")).toMatchObject({
        outputTokens: 60,
        context: null,
      });
      handleAgentEvent(host, context);
      expect(host.chatRunUsageById?.get("native-run")?.context).toBeNull();
    },
  );

  it("retains native context-only events without inventing output usage", () => {
    const host = createHost({ chatRunId: "native-run" });
    handleAgentEvent(
      host,
      agentEvent(
        "native-run",
        1,
        "usage",
        {
          activeContextTokens: 105_455,
          modelContextWindow: 258_400,
          inputTokens: 104_000,
        },
        "main",
      ),
    );
    expect(host.chatRunUsageById?.get("native-run")).toEqual({
      seq: 1,
      context: { totalTokens: 105_455, modelContextWindow: 258_400, inputTokens: 104_000 },
    });
  });

  it("keeps context separate from output billing and fences older recovery snapshots", () => {
    const host = createHost({ chatRunId: "native-run" });
    handleAgentEvent(
      host,
      agentEvent(
        "native-run",
        5,
        "usage",
        {
          activeContextTokens: 80_000,
          modelContextWindow: 258_400,
        },
        "main",
      ),
    );
    handleAgentEvent(host, agentEvent("native-run", 6, "usage", { outputTokens: 400_000 }, "main"));
    resetToolStream(host);
    handleAgentEvent(
      host,
      agentEvent(
        "native-run",
        4,
        "usage",
        {
          activeContextTokens: 190_000,
          modelContextWindow: 258_400,
        },
        "main",
      ),
    );
    expect(host.chatRunUsageById?.get("native-run")).toEqual({
      seq: 6,
      outputTokens: 400_000,
      context: { totalTokens: 80_000, modelContextWindow: 258_400 },
    });
  });

  it("invalidates pre-compaction context until a new native observation arrives", () => {
    vi.useFakeTimers();
    vi.stubGlobal("window", { setTimeout, clearTimeout });
    const host = createHost({ chatRunId: "native-run" });
    handleAgentEvent(
      host,
      agentEvent(
        "native-run",
        1,
        "usage",
        {
          activeContextTokens: 200_000,
          outputTokens: 50,
        },
        "main",
      ),
    );
    handleAgentEvent(host, agentEvent("native-run", 2, "compaction", { phase: "start" }, "main"));
    handleAgentEvent(host, agentEvent("native-run", 3, "usage", { outputTokens: 60 }, "main"));
    expect(host.chatRunUsageById?.get("native-run")?.context).toBeNull();
    handleAgentEvent(
      host,
      agentEvent(
        "native-run",
        4,
        "usage",
        {
          activeContextTokens: 20_000,
          modelContextWindow: 258_400,
        },
        "main",
      ),
    );
    expect(host.chatRunUsageById?.get("native-run")).toMatchObject({
      outputTokens: 60,
      context: { totalTokens: 20_000 },
    });
  });

  it("bounds retained usage while keeping the most recently updated run", () => {
    const host = createHost();
    for (let index = 0; index < 60; index++) {
      handleAgentEvent(
        host,
        agentEvent(`run-${index}`, 1, "usage", { outputTokens: index }, "main"),
      );
      handleAgentEvent(
        host,
        agentEvent("still-active", index + 1, "usage", { outputTokens: index }, "main"),
      );
    }
    expect(host.chatRunUsageById?.size).toBe(50);
    expect(host.chatRunUsageById?.has("run-0")).toBe(false);
    expect(host.chatRunUsageById?.get("still-active")?.outputTokens).toBe(59);
  });

  it("keeps the last usage through completion even without an intervening render", () => {
    const host = createHost({ chatRunId: "client-run" });
    handleAgentEvent(host, agentEvent("client-run", 1, "usage", { outputTokens: 695 }, "main"));
    handleAgentEvent(host, agentEvent("client-run", 2, "lifecycle", { phase: "end" }, "main"));
    expect(host.chatRunUsageById?.get("client-run")?.outputTokens).toBe(695);
  });

  it("accepts a newer corrected count but ignores older recovery usage", () => {
    const host = createHost({ chatRunId: "client-run" });
    handleAgentEvent(host, agentEvent("client-run", 8, "usage", { outputTokens: 120 }, "main"));
    handleAgentEvent(host, agentEvent("client-run", 9, "usage", { outputTokens: 115 }, "main"));
    resetToolStream(host);
    handleAgentEvent(host, agentEvent("client-run", 7, "lifecycle", { phase: "start" }, "main"));
    handleAgentEvent(host, agentEvent("client-run", 7, "usage", { outputTokens: 150 }, "main"));
    expect(host.chatRunUsageById?.get("client-run")?.outputTokens).toBe(115);
  });

  it("tracks sequence-ordered output usage for a session-owned engine run", () => {
    const host = createHost({ chatRunId: "client-run" });

    handleAgentEvent(host, agentEvent("engine-run", 1, "usage", { outputTokens: 12 }, "main"));
    handleAgentEvent(host, agentEvent("engine-run", 2, "usage", { outputTokens: 8 }, "main"));

    expect(host.chatRunUsageById?.get("engine-run")?.outputTokens).toBe(8);

    handleAgentEvent(host, agentEvent("engine-run", 3, "lifecycle", { phase: "start" }, "main"));
    handleAgentEvent(host, agentEvent("engine-run", 4, "usage", { outputTokens: 3 }, "main"));

    expect(host.chatRunUsageById?.get("engine-run")?.outputTokens).toBe(3);
  });

  it("keeps session-scoped usage separate for concurrent active runs", () => {
    const host = createHost();

    handleAgentEvent(host, agentEvent("run-a", 1, "usage", { outputTokens: 100 }, "main"));
    handleAgentEvent(host, agentEvent("run-b", 1, "usage", { outputTokens: 10 }, "main"));

    expect(Array.from(host.chatRunUsageById?.entries() ?? [])).toEqual([
      ["run-a", { outputTokens: 100, seq: 1 }],
      ["run-b", { outputTokens: 10, seq: 1 }],
    ]);
  });

  it("projects provider-independent system warnings into the visible session transcript", () => {
    const host = createHost({ chatRunId: "client-run" });

    expect(
      handleAgentEvent(
        host,
        agentEvent(
          "client-run",
          1,
          "notice",
          { phase: "warning", message: "Custom execution rules were not applied." },
          "main",
        ),
      ),
    ).toBe(true);
    expect(host.guardianNotices).toMatchObject([
      {
        kind: "warning",
        source: "system",
        message: "Custom execution rules were not applied.",
      },
    ]);
  });

  it("replaces a pending targetless Guardian review with its terminal decision", () => {
    const host = createHost({ chatRunId: "client-run" });
    const review = {
      reviewId: "network-review",
      targetItemId: null,
      command: "https://api.example.test:443",
    };

    handleAgentEvent(
      host,
      agentEvent(
        "client-run",
        1,
        "codex_app_server.guardian",
        { ...review, phase: "started", status: "inProgress" },
        "main",
      ),
    );
    expect(host.guardianNotices).toMatchObject([
      { kind: "reviewing", command: "https://api.example.test:443" },
    ]);

    handleAgentEvent(
      host,
      agentEvent(
        "client-run",
        2,
        "codex_app_server.guardian",
        { ...review, phase: "completed", status: "denied" },
        "main",
      ),
    );
    expect(host.guardianNotices).toMatchObject([
      { kind: "denied", command: "https://api.example.test:443" },
    ]);
  });

  it("shows a targeted strict-review requirement only until its decision arrives", () => {
    const host = createHost({ chatRunId: "client-run" });
    const review = {
      reviewId: "strict-review",
      targetItemId: "command-1",
      command: "printf hello",
    };

    handleAgentEvent(
      host,
      agentEvent(
        "client-run",
        1,
        "codex_app_server.guardian",
        { ...review, phase: "strict_review_required" },
        "main",
      ),
    );
    expect(host.guardianNotices).toMatchObject([
      { kind: "strict-review-required", command: "printf hello" },
    ]);

    handleAgentEvent(
      host,
      agentEvent(
        "client-run",
        2,
        "codex_app_server.guardian",
        { ...review, phase: "completed", status: "approved" },
        "main",
      ),
    );
    expect(host.guardianNotices).toEqual([]);
  });

  it("rejects a sessionless system notice from a foreign run", () => {
    const host = createHost({ chatRunId: "client-run" });

    expect(
      handleAgentEvent(
        host,
        agentEvent("foreign-run", 1, "notice", {
          phase: "warning",
          message: "Foreign system warning",
        }),
      ),
    ).toBe(true);
    expect(host.guardianNotices).toEqual([]);
  });

  it("rejects a same-session Guardian notice from a foreign run", () => {
    const host = createHost({ chatRunId: "client-run" });

    expect(
      handleAgentEvent(
        host,
        agentEvent(
          "foreign-run",
          1,
          "codex_app_server.guardian",
          {
            reviewId: "foreign-review",
            phase: "started",
            status: "inProgress",
            command: "foreign command",
            rationale: "foreign rationale",
          },
          "main",
        ),
      ),
    ).toBe(true);
    expect(host.guardianNotices).toEqual([]);
  });

  it("requires the local run id when an event has no session identity", () => {
    const host = createHost({ chatRunId: "client-run" });

    handleAgentEvent(host, agentEvent("engine-run", 1, "usage", { outputTokens: 20 }));
    handleAgentEvent(host, agentEvent("client-run", 2, "usage", { outputTokens: 7 }));

    expect(Array.from(host.chatRunUsageById?.entries() ?? [])).toEqual([
      ["client-run", { outputTokens: 7, seq: 2 }],
    ]);
  });
});
