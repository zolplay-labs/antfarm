import { describe, it, afterEach } from "node:test";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import { getDb } from "../db.js";
import {
  parseOutputKeyValues,
  resolveTemplate,
  claimStep,
  completeStep,
} from "./step-ops.js";

// ── Helpers ─────────────────────────────────────────────────────────

const testRunIds: string[] = [];

function cleanup() {
  const db = getDb();
  for (const id of testRunIds) {
    db.prepare("DELETE FROM stories WHERE run_id = ?").run(id);
    db.prepare("DELETE FROM steps WHERE run_id = ?").run(id);
    db.prepare("DELETE FROM runs WHERE id = ?").run(id);
  }
  testRunIds.length = 0;
}

function createRun(opts: {
  runId: string;
  context?: Record<string, string>;
  status?: string;
}) {
  const db = getDb();
  const now = new Date().toISOString();
  db.prepare(
    "INSERT INTO runs (id, run_number, workflow_id, task, status, context, created_at, updated_at) VALUES (?, 1, 'test-wf', 'test task', ?, ?, ?, ?)"
  ).run(opts.runId, opts.status ?? "running", JSON.stringify(opts.context ?? {}), now, now);
  testRunIds.push(opts.runId);
}

function createStep(opts: {
  runId: string;
  stepId: string;
  agentId: string;
  stepIndex: number;
  inputTemplate?: string;
  status?: string;
  type?: string;
  loopConfig?: string | null;
}): string {
  const db = getDb();
  const now = new Date().toISOString();
  const id = crypto.randomUUID();
  db.prepare(
    "INSERT INTO steps (id, run_id, step_id, agent_id, step_index, input_template, expects, status, type, loop_config, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, '', ?, ?, ?, ?, ?)"
  ).run(
    id,
    opts.runId,
    opts.stepId,
    opts.agentId,
    opts.stepIndex,
    opts.inputTemplate ?? "",
    opts.status ?? "waiting",
    opts.type ?? "single",
    opts.loopConfig ?? null,
    now,
    now
  );
  return id;
}

function createStory(opts: {
  runId: string;
  storyIndex: number;
  storyId: string;
  title: string;
  status?: string;
}): string {
  const db = getDb();
  const now = new Date().toISOString();
  const id = crypto.randomUUID();
  db.prepare(
    "INSERT INTO stories (id, run_id, story_index, story_id, title, description, acceptance_criteria, status, retry_count, max_retries, created_at, updated_at) VALUES (?, ?, ?, ?, ?, '', '[]', ?, 0, 2, ?, ?)"
  ).run(id, opts.runId, opts.storyIndex, opts.storyId, opts.title, opts.status ?? "pending", now, now);
  return id;
}

// ── parseOutputKeyValues ────────────────────────────────────────────

describe("parseOutputKeyValues", () => {
  it("parses setup step output with KEY: value lines", () => {
    const output = [
      "STATUS: done",
      "WORKTREE: /tmp/antfarm-abc123",
      "BUILD_CMD: npm run build",
      "TEST_CMD: npm test",
      "CI_NOTES: uses vitest",
      "BASELINE: all tests pass",
    ].join("\n");

    const result = parseOutputKeyValues(output);
    assert.equal(result["status"], "done");
    assert.equal(result["worktree"], "/tmp/antfarm-abc123");
    assert.equal(result["build_cmd"], "npm run build");
    assert.equal(result["test_cmd"], "npm test");
    assert.equal(result["ci_notes"], "uses vitest");
    assert.equal(result["baseline"], "all tests pass");
  });

  it("converts keys to lowercase", () => {
    const output = "WORKTREE: /tmp/test\nBUILD_CMD: make build";
    const result = parseOutputKeyValues(output);
    assert.ok("worktree" in result);
    assert.ok("build_cmd" in result);
    assert.ok(!("WORKTREE" in result));
    assert.ok(!("BUILD_CMD" in result));
  });

  it("skips STORIES_JSON keys", () => {
    const output = 'STORIES_JSON: [{"id": "1"}]\nSTATUS: done';
    const result = parseOutputKeyValues(output);
    assert.ok(!("stories_json" in result));
    assert.equal(result["status"], "done");
  });

  it("handles multi-line values", () => {
    const output = [
      "STATUS: done",
      "CI_NOTES: line one",
      "  continuation line two",
      "  continuation line three",
      "BASELINE: pass",
    ].join("\n");

    const result = parseOutputKeyValues(output);
    assert.ok(result["ci_notes"].includes("line one"));
    assert.ok(result["ci_notes"].includes("continuation line two"));
    assert.equal(result["baseline"], "pass");
  });

  it("handles output with non-KEY lines interspersed", () => {
    const output = [
      "Some log output from the agent",
      "More logs here",
      "STATUS: done",
      "WORKTREE: /tmp/test",
    ].join("\n");

    const result = parseOutputKeyValues(output);
    assert.equal(result["status"], "done");
    assert.equal(result["worktree"], "/tmp/test");
  });
});

// ── resolveTemplate ─────────────────────────────────────────────────

describe("resolveTemplate", () => {
  it("resolves lowercase template keys from lowercase context", () => {
    const template = "WORKTREE: {{worktree}}\nBUILD_CMD: {{build_cmd}}";
    const context = { worktree: "/tmp/test", build_cmd: "npm run build" };
    const result = resolveTemplate(template, context);
    assert.equal(result, "WORKTREE: /tmp/test\nBUILD_CMD: npm run build");
  });

  it("falls back to lowercase context lookup for uppercase template keys", () => {
    const template = "{{WORKTREE}}";
    const context = { worktree: "/tmp/test" };
    const result = resolveTemplate(template, context);
    assert.equal(result, "/tmp/test");
  });

  it("marks missing keys", () => {
    const template = "{{worktree}} {{missing_key}}";
    const context = { worktree: "/tmp/test" };
    const result = resolveTemplate(template, context);
    assert.ok(result.includes("/tmp/test"));
    assert.ok(result.includes("[missing: missing_key]"));
  });
});

// ── claimStep: verify_each flow ─────────────────────────────────────

describe("claimStep verify_each", () => {
  afterEach(cleanup);

  it("allows verify step to be claimed while loop step is running", () => {
    const runId = crypto.randomUUID();
    createRun({
      runId,
      context: {
        task: "test task",
        repo: "/tmp/repo",
        branch: "feat-test",
        worktree: "/tmp/antfarm-test",
        build_cmd: "npm run build",
        test_cmd: "npm test",
        changes: "implemented feature X",
        current_story: "Story 1",
        current_story_id: "S-1",
        current_story_title: "Story 1",
      },
    });

    const loopConfig = JSON.stringify({
      over: "stories",
      completion: "all_done",
      freshSession: true,
      verifyEach: true,
      verifyStep: "verify",
    });

    // Create steps: plan (done), setup (done), implement (running loop), verify (pending)
    createStep({ runId, stepId: "plan", agentId: "test-wf_planner", stepIndex: 0, status: "done" });
    createStep({ runId, stepId: "setup", agentId: "test-wf_setup", stepIndex: 1, status: "done" });
    createStep({
      runId,
      stepId: "implement",
      agentId: "test-wf_developer",
      stepIndex: 2,
      status: "running",
      type: "loop",
      loopConfig,
    });

    const verifyTemplate = "WORKTREE: {{worktree}}\nTEST_CMD: {{test_cmd}}\nCHANGES: {{changes}}";
    createStep({
      runId,
      stepId: "verify",
      agentId: "test-wf_verifier",
      stepIndex: 3,
      status: "pending",
      inputTemplate: verifyTemplate,
    });

    // The verifier should be able to claim the verify step
    const result = claimStep("test-wf_verifier");
    assert.equal(result.found, true, "verify step should be claimable while loop step is running");
    assert.ok(result.resolvedInput?.includes("/tmp/antfarm-test"), "resolved input should contain worktree");
    assert.ok(result.resolvedInput?.includes("npm test"), "resolved input should contain test_cmd");
  });

  it("still blocks non-verify steps from claiming while a previous step is running", () => {
    const runId = crypto.randomUUID();
    createRun({ runId, context: { task: "test task" } });

    // step-a is running (non-loop), step-b is pending
    createStep({ runId, stepId: "step-a", agentId: "test-wf_agent-a", stepIndex: 0, status: "running" });
    createStep({ runId, stepId: "step-b", agentId: "test-wf_agent-b", stepIndex: 1, status: "pending" });

    const result = claimStep("test-wf_agent-b");
    assert.equal(result.found, false, "step-b should NOT be claimable while non-loop step-a is running");
  });
});

// ── completeStep: context propagation ───────────────────────────────

describe("completeStep context propagation", () => {
  afterEach(cleanup);

  it("merges setup output keys into run context for downstream steps", () => {
    const runId = crypto.randomUUID();
    createRun({ runId, context: { task: "test task", repo: "/tmp/repo", branch: "feat-x" } });

    // Create setup step (pending → running), and implement step (waiting)
    const setupStepId = createStep({
      runId,
      stepId: "setup",
      agentId: "test-wf_setup",
      stepIndex: 0,
      status: "running",
    });

    createStep({
      runId,
      stepId: "implement",
      agentId: "test-wf_developer",
      stepIndex: 1,
      status: "waiting",
      type: "loop",
      loopConfig: JSON.stringify({ over: "stories", completion: "all_done", freshSession: true, verifyEach: true, verifyStep: "verify" }),
      inputTemplate: "WORKTREE: {{worktree}}\nBUILD_CMD: {{build_cmd}}\nTEST_CMD: {{test_cmd}}",
    });

    // Setup step completes with KEY: VALUE output
    const setupOutput = [
      "STATUS: done",
      "WORKTREE: /tmp/antfarm-run123",
      "BUILD_CMD: pnpm build",
      "TEST_CMD: pnpm test",
      "CI_NOTES: uses turborepo",
      "BASELINE: 42 tests pass",
    ].join("\n");

    completeStep(setupStepId, setupOutput);

    // Verify context was saved with parsed keys
    const db = getDb();
    const run = db.prepare("SELECT context FROM runs WHERE id = ?").get(runId) as { context: string };
    const context = JSON.parse(run.context);
    assert.equal(context["worktree"], "/tmp/antfarm-run123");
    assert.equal(context["build_cmd"], "pnpm build");
    assert.equal(context["test_cmd"], "pnpm test");
    assert.equal(context["ci_notes"], "uses turborepo");

    // Verify the implement step was advanced to pending
    const implementStep = db.prepare(
      "SELECT status FROM steps WHERE run_id = ? AND step_id = 'implement'"
    ).get(runId) as { status: string };
    assert.equal(implementStep.status, "pending");
  });
});
