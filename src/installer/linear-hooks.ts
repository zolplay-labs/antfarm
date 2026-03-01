/**
 * Linear adapter hooks for antfarm workflows.
 *
 * Handles:
 * - Exporting Linear project/issue data to feature-dev stories format
 * - Syncing workflow status back to Linear (state transitions, comments, PR links)
 * - Creating Linear issues from planner output (blank-slate mode)
 */

import { execFileSync } from "node:child_process";
import { getDb } from "../db.js";
import { logger } from "../lib/logger.js";

// ── Types ───────────────────────────────────────────────────────────

export interface LinearIssue {
  id: string;
  identifier: string;
  title: string;
  description: string;
  priority: number;
  state: { id: string; name: string };
  team: { id: string; name: string };
  labels: Array<{ id: string; name: string }>;
  sortOrder: number;
}

export interface LinearStory {
  id: string;
  title: string;
  description: string;
  acceptanceCriteria: string[];
  linearIssueId: string;
  linearIdentifier: string;
  teamId: string;
}

export interface LinearConfig {
  source: "project" | "issue";
  sourceId: string;
  teamId?: string;
  approve: boolean;
}

// ── State mapping ───────────────────────────────────────────────────

/**
 * Map of workflow events to target Linear state names.
 * The actual state IDs are resolved per-team at runtime.
 */
const STATE_MAP: Record<string, string> = {
  "story.started": "In Progress",
  "story.done": "In Review",
  "pr.created": "Done",
};

// ── Linear CLI helpers ──────────────────────────────────────────────

function linearExec(args: string[], json = false): string {
  const fullArgs = [...args];
  if (json) fullArgs.push("--json");
  try {
    return execFileSync("linear", fullArgs, {
      encoding: "utf-8",
      timeout: 30_000,
    }).trim();
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    throw new Error(`linear CLI failed: ${fullArgs.join(" ")} — ${msg}`);
  }
}

function linearJson<T>(args: string[]): T {
  const output = linearExec(args, true);
  return JSON.parse(output) as T;
}

// ── State resolution ────────────────────────────────────────────────

let _stateCache: Map<string, Map<string, string>> | null = null;

/**
 * Get workflow states for a team, cached.
 * Returns Map<stateName (lowercase), stateId>.
 */
function getTeamStates(teamId: string): Map<string, string> {
  if (!_stateCache) _stateCache = new Map();
  if (_stateCache.has(teamId)) return _stateCache.get(teamId)!;

  const states = linearJson<Array<{ id: string; name: string; team: { id: string } }>>(
    ["state", "list", "--team", teamId]
  );
  const map = new Map<string, string>();
  for (const s of states) {
    map.set(s.name.toLowerCase(), s.id);
  }
  _stateCache.set(teamId, map);
  return map;
}

/**
 * Resolve a state name to a state ID for a given team.
 */
function resolveStateId(teamId: string, stateName: string): string | null {
  const states = getTeamStates(teamId);
  return states.get(stateName.toLowerCase()) ?? null;
}

// ── Export: Linear → Stories ────────────────────────────────────────

/**
 * Export Linear project issues to feature-dev stories format.
 */
export function exportProjectStories(projectId: string): LinearStory[] {
  const issues = linearJson<LinearIssue[]>(["project", "issues", projectId]);

  if (!issues || issues.length === 0) {
    throw new Error(`No issues found in Linear project ${projectId}`);
  }

  // Sort by priority (1=urgent, 4=low, 0=no priority) then by sortOrder
  const sorted = [...issues].sort((a, b) => {
    const pa = a.priority === 0 ? 5 : a.priority;
    const pb = b.priority === 0 ? 5 : b.priority;
    if (pa !== pb) return pa - pb;
    return a.sortOrder - b.sortOrder;
  });

  return sorted.map((issue, i) => ({
    id: `S${String(i + 1).padStart(2, "0")}`,
    title: `[${issue.identifier}] ${issue.title}`,
    description: issue.description || issue.title,
    acceptanceCriteria: parseAcceptanceCriteria(issue.description || issue.title),
    linearIssueId: issue.id,
    linearIdentifier: issue.identifier,
    teamId: issue.team?.id ?? "",
  }));
}

/**
 * Export a single Linear issue as a story.
 */
export function exportIssueStory(issueId: string): LinearStory[] {
  const issue = linearJson<LinearIssue>(["issue", "get", issueId]);

  return [{
    id: "S01",
    title: `[${issue.identifier}] ${issue.title}`,
    description: issue.description || issue.title,
    acceptanceCriteria: parseAcceptanceCriteria(issue.description || issue.title),
    linearIssueId: issue.id,
    linearIdentifier: issue.identifier,
    teamId: issue.team?.id ?? "",
  }];
}

/**
 * Export multiple Linear issues as ordered stories.
 * Each issue identifier is fetched individually and returned in the provided order.
 */
export function exportIssueStories(issueIds: string[]): LinearStory[] {
  return issueIds.map((issueId, i) => {
    const issue = linearJson<LinearIssue>(["issue", "get", issueId]);
    return {
      id: `S${String(i + 1).padStart(2, "0")}`,
      title: `[${issue.identifier}] ${issue.title}`,
      description: issue.description || issue.title,
      acceptanceCriteria: parseAcceptanceCriteria(issue.description || issue.title),
      linearIssueId: issue.id,
      linearIdentifier: issue.identifier,
      teamId: issue.team?.id ?? "",
    };
  });
}

/**
 * Parse acceptance criteria from issue description.
 * Looks for checkbox items (- [ ] ...) or numbered lists, falling back to generic criteria.
 */
function parseAcceptanceCriteria(description: string): string[] {
  const criteria: string[] = [];

  // Extract checkbox items
  const checkboxRe = /^[-*]\s*\[[ x]\]\s*(.+)$/gm;
  let match: RegExpExecArray | null;
  while ((match = checkboxRe.exec(description)) !== null) {
    criteria.push(match[1].trim());
  }

  if (criteria.length > 0) {
    if (!criteria.some(c => c.toLowerCase() === "typecheck passes")) {
      criteria.push("Typecheck passes");
    }
    return criteria;
  }

  // Extract numbered list items
  const numberedRe = /^\d+\.\s+(.+)$/gm;
  while ((match = numberedRe.exec(description)) !== null) {
    criteria.push(match[1].trim());
  }

  if (criteria.length > 0) {
    if (!criteria.some(c => c.toLowerCase() === "typecheck passes")) {
      criteria.push("Typecheck passes");
    }
    return criteria;
  }

  // Fallback
  return [
    "Implementation matches the issue description",
    "Tests for the feature pass",
    "Typecheck passes",
  ];
}

// ── Import: Stories → Linear (blank-slate mode) ─────────────────────

export interface CreateLinearIssuesParams {
  teamId: string;
  projectId?: string;
  stories: Array<{
    id: string;
    title: string;
    description: string;
    acceptanceCriteria: string[];
  }>;
  labelIds?: string[];
}

/**
 * Create Linear issues from planner stories (blank-slate mode).
 * Returns updated stories with Linear issue IDs.
 */
export function createLinearIssues(params: CreateLinearIssuesParams): LinearStory[] {
  const results: LinearStory[] = [];

  for (const story of params.stories) {
    const acText = story.acceptanceCriteria.map((c, i) => `${i + 1}. ${c}`).join("\n");
    const description = `${story.description}\n\n## Acceptance Criteria\n${acText}`;

    const createArgs = [
      "issue", "create",
      "--title", story.title,
      "--description", description,
      "--team", params.teamId,
    ];

    if (params.projectId) {
      createArgs.push("--project", params.projectId);
    }

    if (params.labelIds) {
      for (const labelId of params.labelIds) {
        createArgs.push("--label", labelId);
      }
    }

    const created = linearJson<{ id: string; identifier: string }>(createArgs);

    results.push({
      id: story.id,
      title: `[${created.identifier}] ${story.title}`,
      description: story.description,
      acceptanceCriteria: story.acceptanceCriteria,
      linearIssueId: created.id,
      linearIdentifier: created.identifier,
      teamId: params.teamId,
    });
  }

  return results;
}

// ── Status sync hooks ───────────────────────────────────────────────

/**
 * Move a Linear issue to a new state.
 */
export function moveIssue(issueId: string, stateName: string, teamId?: string): void {
  try {
    if (teamId) {
      const stateId = resolveStateId(teamId, stateName);
      if (stateId) {
        linearExec(["issue", "move", issueId, stateId]);
        return;
      }
    }
    // Fallback: try by state name directly (some CLI versions support this)
    linearExec(["issue", "move", issueId, stateName]);
  } catch (err) {
    logger.warn(`Failed to move Linear issue ${issueId} to ${stateName}: ${err}`);
  }
}

/**
 * Add a comment to a Linear issue.
 */
export function addComment(issueId: string, body: string): void {
  try {
    linearExec(["comment", "create", issueId, "--body", body]);
  } catch (err) {
    logger.warn(`Failed to comment on Linear issue ${issueId}: ${err}`);
  }
}

/**
 * Link a PR to a Linear issue via comment.
 */
export function linkPR(issueId: string, prUrl: string): void {
  addComment(issueId, `🔗 Pull Request: ${prUrl}`);
}

// ── Run-level hooks (called from step-ops) ──────────────────────────

/**
 * Get the Linear issue mapping for a run from the run context.
 * Returns Map<storyId, { linearIssueId, linearIdentifier, teamId }>.
 */
export function getLinearMapping(runId: string): Map<string, { linearIssueId: string; linearIdentifier: string; teamId?: string }> | null {
  const db = getDb();
  const run = db.prepare("SELECT context FROM runs WHERE id = ?").get(runId) as { context: string } | undefined;
  if (!run) return null;

  const context = JSON.parse(run.context);
  const mapping = context["linear_mapping"];
  if (!mapping) return null;

  try {
    const parsed = JSON.parse(mapping) as Array<{ storyId: string; linearIssueId: string; linearIdentifier: string; teamId?: string }>;
    const map = new Map<string, { linearIssueId: string; linearIdentifier: string; teamId?: string }>();
    for (const entry of parsed) {
      map.set(entry.storyId, entry);
    }
    return map;
  } catch {
    return null;
  }
}

/**
 * Hook: called when a story starts execution.
 */
export function onStoryStarted(runId: string, storyId: string): void {
  const mapping = getLinearMapping(runId);
  if (!mapping) return;

  const entry = mapping.get(storyId);
  if (!entry) return;

  moveIssue(entry.linearIssueId, STATE_MAP["story.started"], entry.teamId);
  addComment(entry.linearIssueId, `🚀 Implementation started by antfarm`);
}

/**
 * Hook: called when a story is verified/done.
 */
export function onStoryDone(runId: string, storyId: string): void {
  const mapping = getLinearMapping(runId);
  if (!mapping) return;

  const entry = mapping.get(storyId);
  if (!entry) return;

  moveIssue(entry.linearIssueId, STATE_MAP["story.done"], entry.teamId);
  addComment(entry.linearIssueId, `✅ Implementation complete, pending review`);
}

/**
 * Hook: called when a PR is created for the run.
 */
export function onPRCreated(runId: string, prUrl: string): void {
  const mapping = getLinearMapping(runId);
  if (!mapping) return;

  for (const [, entry] of mapping) {
    moveIssue(entry.linearIssueId, STATE_MAP["pr.created"], entry.teamId);
    linkPR(entry.linearIssueId, prUrl);
  }
}

/**
 * Hook: called when a story fails.
 */
export function onStoryFailed(runId: string, storyId: string, error: string): void {
  const mapping = getLinearMapping(runId);
  if (!mapping) return;

  const entry = mapping.get(storyId);
  if (!entry) return;

  addComment(entry.linearIssueId, `❌ Implementation failed: ${error}`);
}

// ── Label management ────────────────────────────────────────────────

/**
 * Apply a label to an existing Linear issue.
 */
export function applyLabel(issueId: string, labelId: string): void {
  try {
    linearJson(["issue", "update", issueId, "--label", labelId]);
  } catch (err) {
    logger.warn(`Failed to apply label ${labelId} to issue ${issueId}: ${err}`);
  }
}

export function ensureLabel(name: string): string | null {
  try {
    const labels = linearJson<Array<{ id: string; name: string }>>(["label", "list"]);
    const existing = labels.find(l => l.name === name);
    if (existing) return existing.id;

    const created = linearJson<{ id: string }>(["label", "create", "--name", name]);
    return created.id;
  } catch (err) {
    logger.warn(`Failed to ensure Linear label "${name}": ${err}`);
    return null;
  }
}
