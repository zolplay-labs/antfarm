import crypto from "node:crypto";
import { loadWorkflowSpec } from "./workflow-spec.js";
import { resolveWorkflowDir } from "./paths.js";
import { getDb, nextRunNumber } from "../db.js";
import { logger } from "../lib/logger.js";
import { ensureWorkflowCrons } from "./agent-cron.js";
import { emitEvent } from "./events.js";
import {
  exportProjectStories,
  exportIssueStory,
  exportIssueStories,
  ensureLabel,
  applyLabel,
  type LinearStory,
  type LinearConfig,
} from "./linear-hooks.js";

export interface RunWorkflowParams {
  workflowId: string;
  taskTitle: string;
  notifyUrl?: string;
  storiesFrom?: string;   // "linear:<project-id>", "linear-issue:<issue-id>", or "linear-issues:<id1>,<id2>,..."
  repo?: string;           // explicit --repo flag
  approve?: boolean;       // pause after export for human approval
  linearTeam?: string;     // team ID for blank-slate mode
  linearProject?: string;  // project ID for blank-slate mode
}

export async function runWorkflow(params: RunWorkflowParams): Promise<{ id: string; runNumber: number; workflowId: string; task: string; status: string }> {
  const workflowDir = resolveWorkflowDir(params.workflowId);
  const workflow = await loadWorkflowSpec(workflowDir);
  const db = getDb();
  const now = new Date().toISOString();
  const runId = crypto.randomUUID();
  const runNumber = nextRunNumber();

  const initialContext: Record<string, string> = {
    task: params.taskTitle,
    ...workflow.context,
  };

  // If --repo provided, inject into context
  if (params.repo) {
    initialContext["repo"] = params.repo;
  }

  // ── Linear pre-processing ──────────────────────────────────────────
  let linearStories: LinearStory[] | null = null;
  let linearMapping: Array<{ storyId: string; linearIssueId: string; linearIdentifier: string; teamId?: string }> | null = null;
  let skipPlanStep = false;

  if (params.storiesFrom) {
    const [source, sourceId] = params.storiesFrom.split(":", 2);
    if (!sourceId) {
      throw new Error(`Invalid --stories-from format: "${params.storiesFrom}". Expected "linear:<project-id>" or "linear-issue:<issue-id>".`);
    }

    if (source === "linear") {
      linearStories = exportProjectStories(sourceId);
      skipPlanStep = true;
    } else if (source === "linear-issue") {
      linearStories = exportIssueStory(sourceId);
      skipPlanStep = true;
    } else if (source === "linear-issues") {
      const issueIds = sourceId.split(",").map(id => id.trim()).filter(Boolean);
      if (issueIds.length === 0) {
        throw new Error(`No issue IDs provided in --stories-from "linear-issues:...".`);
      }
      linearStories = exportIssueStories(issueIds);
      skipPlanStep = true;
    } else {
      throw new Error(`Unknown stories source: "${source}". Supported: "linear", "linear-issue", "linear-issues".`);
    }

    // Ensure wbs/nick label and apply to imported issues
    const labelId = ensureLabel("wbs/nick");
    if (labelId) {
      for (const s of linearStories) {
        applyLabel(s.linearIssueId, labelId);
      }
    }

    // Build Linear mapping for status sync hooks (include teamId for reliable state resolution)
    linearMapping = linearStories.map(s => ({
      storyId: s.id,
      linearIssueId: s.linearIssueId,
      linearIdentifier: s.linearIdentifier,
      teamId: s.teamId,
    }));

    initialContext["linear_mapping"] = JSON.stringify(linearMapping);
    initialContext["linear_source"] = params.storiesFrom;

    // Auto-generate branch name when planner is skipped (stories imported directly)
    if (!initialContext["branch"]) {
      const slug = params.taskTitle.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "").slice(0, 60);
      initialContext["branch"] = `feat/${slug}`;
    }

    logger.info(`Imported ${linearStories.length} stories from Linear (${params.storiesFrom})`, { workflowId: workflow.id });
  }

  // Blank-slate mode: planner runs, then we create Linear issues from output
  if (!params.storiesFrom && params.linearTeam) {
    initialContext["linear_blank_slate"] = "true";
    initialContext["linear_team_id"] = params.linearTeam;
    if (params.linearProject) {
      initialContext["linear_project_id"] = params.linearProject;
    }
  }

  // Determine initial run status
  const runStatus = (params.approve && (linearStories || skipPlanStep)) ? "paused" : "running";

  db.exec("BEGIN");
  try {
    const notifyUrl = params.notifyUrl ?? workflow.notifications?.url ?? null;
    const insertRun = db.prepare(
      "INSERT INTO runs (id, run_number, workflow_id, task, status, context, notify_url, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    );
    insertRun.run(runId, runNumber, workflow.id, params.taskTitle, runStatus, JSON.stringify(initialContext), notifyUrl, now, now);

    const insertStep = db.prepare(
      "INSERT INTO steps (id, run_id, step_id, agent_id, step_index, input_template, expects, status, max_retries, type, loop_config, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    );

    // Filter steps: skip plan step if stories are pre-imported
    let stepIndex = 0;
    for (let i = 0; i < workflow.steps.length; i++) {
      const step = workflow.steps[i];

      // Skip plan step when stories come from Linear
      if (skipPlanStep && step.id === "plan") {
        continue;
      }

      const stepUuid = crypto.randomUUID();
      const agentId = `${workflow.id}_${step.agent}`;
      const status = stepIndex === 0 ? (runStatus === "paused" ? "waiting" : "pending") : "waiting";
      const maxRetries = step.max_retries ?? step.on_fail?.max_retries ?? 2;
      const stepType = step.type ?? "single";
      const loopConfig = step.loop ? JSON.stringify(step.loop) : null;
      insertStep.run(stepUuid, runId, step.id, agentId, stepIndex, step.input, step.expects, status, maxRetries, stepType, loopConfig, now, now);
      stepIndex++;
    }

    // Insert pre-imported stories
    if (linearStories) {
      const insertStory = db.prepare(
        "INSERT INTO stories (id, run_id, story_index, story_id, title, description, acceptance_criteria, status, retry_count, max_retries, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', 0, 2, ?, ?)"
      );
      for (let i = 0; i < linearStories.length; i++) {
        const s = linearStories[i];
        insertStory.run(crypto.randomUUID(), runId, i, s.id, s.title, s.description, JSON.stringify(s.acceptanceCriteria), now, now);
      }
    }

    db.exec("COMMIT");
  } catch (err) {
    db.exec("ROLLBACK");
    throw err;
  }

  // Start crons for this workflow (no-op if already running from another run)
  try {
    await ensureWorkflowCrons(workflow);
  } catch (err) {
    // Roll back the run since it can't advance without crons
    const db2 = getDb();
    db2.prepare("UPDATE runs SET status = 'failed', updated_at = ? WHERE id = ?").run(new Date().toISOString(), runId);
    const message = err instanceof Error ? err.message : String(err);
    throw new Error(`Cannot start workflow run: cron setup failed. ${message}`);
  }

  emitEvent({ ts: new Date().toISOString(), event: "run.started", runId, workflowId: workflow.id });

  logger.info(`Run started: "${params.taskTitle}"`, {
    workflowId: workflow.id,
    runId,
    stepId: workflow.steps[0]?.id,
  });

  return { id: runId, runNumber, workflowId: workflow.id, task: params.taskTitle, status: "running" };
}
