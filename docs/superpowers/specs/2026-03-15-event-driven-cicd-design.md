# Event-Driven CI/CD Sub-processes

**Date:** 2026-03-15
**Status:** Approved

## Problem

Feature-to-production pipeline (v5.0) is a partial orchestrator: key blocks (review, deploy, verify) are Call Activities, but merge/comment/notification logic is inline. Sub-processes (pr-review, deploy-process) can only be triggered by the pipeline — not independently by GitHub events.

This means:
- Every PR review requires a running pipeline instance
- Deploy to staging only happens through the pipeline, not from other merge sources (main-to-staging-sync, upstream-sync)
- Three separate processes (feature-to-production, main-to-staging-sync, upstream-sync) each independently call deploy-process as a Call Activity — duplicated orchestration

## Design

### Core Principle

Sub-processes become autonomous, event-driven processes triggered by GitHub webhooks. Pipeline becomes a pure orchestrator that waits for results via Zeebe message correlation instead of invoking Call Activities.

Sub-processes always publish a result message at the end. If pipeline is listening — it catches it. If standalone — message expires after TTL. Sub-processes do not need to know who started them.

### Architecture Overview

```
GitHub Events
    │
    ├── pull_request opened/reopened ──► webhook ──► msg_pr_event ──► pr-review (standalone)
    │                                                                     │
    │                                                                     ▼
    │                                                              msg_review_done [pr_number]
    │                                                                     │
    │                                                          pipeline catches (if running)
    │
    ├── pull_request synchronize ──► webhook ──► msg_pr_updated ──► pr-review catches (rework loop)
    │
    └── push to staging ──► webhook ──► msg_deploy_trigger [sha] ──► deploy-process (standalone)
                                                                          │
                                                                          ▼
                                                                   msg_deploy_done [sha]
                                                                          │
                                                               pipeline catches (if running)
```

### Zeebe Message Delivery Model (8.8)

In Zeebe 8.8, a single published message can simultaneously:
1. **Start a new process instance** via Message Start Event (matches by message name; correlation key acts as business key for deduplication)
2. **Correlate to a running instance** via Message Intermediate Catch Event (matches by message name + correlation key)

These are independent subscription types. This means `msg_pr_event` published once will both start a pr-review instance AND be caught by a waiting pipeline instance. This dual-delivery is the foundation of the design.

### PR Review Process (`pr-review.bpmn`)

**Changes from current:**
- Start Event: None → Message Start Event (`msg_pr_event`, correlation = `head_branch` as business key for deduplication — prevents duplicate reviews for same branch)
- Add Message End Event: publish `msg_review_done` (correlation = `pr_number`, variables: `review_score`, `has_critical_issues`)
- All existing logic unchanged: PR-Agent review, score check loop, rework wait, reminder timer
- No XOR gateway for `odoo_task_id` — this is a CI/CD sub-process that never creates its own Odoo tasks; the `odoo_task_id` check rule applies to top-level business processes

**Flow:**
```
Message Start Event "msg_pr_event" [business_key=head_branch]
  → PR-Agent review (Service Task, job: pr-agent-review)
  → XOR: score >= 7 and no critical issues?
      ├── Yes → Message End Event: publish "msg_review_done" [correlation=pr_number]
      └── No  → comment "потребує доопрацювання" (github-comment)
               → Receive Task: wait "msg_pr_updated" [correlation=pr_number]
                   ├── Boundary Timer (R/P3D, non-interrupting) → reminder comment
               → loop back to PR-Agent review
```

**Output variables in `msg_review_done`:**
- `review_score` (int, 0-10)
- `has_critical_issues` (bool)

### Deploy Process (`deploy-process.bpmn`)

**Two start events (standard Zeebe 8.8 pattern):**
- Message Start Event `msg_deploy_trigger` — for webhook-triggered deploys (staging)
- None Start Event — for Call Activity invocations (demo deploy from upstream-sync)

When deployed, Zeebe creates a message subscription at the process-definition level for `msg_deploy_trigger`. This does not interfere with Call Activity invocations — they use the None Start Event.

**Changes from current:**
- Add Message Start Event `msg_deploy_trigger` (correlation = `trigger_sha`)
- Keep None Start Event (for Call Activity from upstream-sync demo deploy)
- Add Message End Event: publish `msg_deploy_done` (correlation = `trigger_sha`, variables: deploy results)
- Error Event Subprocess: publishes `msg_deploy_done` with `deploy_failed=true` AND throws BPMN Error `DEPLOY_FAILED` (see Error Handling section)
- All existing deploy logic unchanged

**Flow:**
```
Message Start Event "msg_deploy_trigger" [trigger_sha]
  (or None Start Event when called via Call Activity)
  → git pull → detect modules
  → XOR: changes? → No → publish "msg_deploy_done" [trigger_sha] (no_changes=true) → End
  → docker build (if needed) → module update → docker up → cache clear
  → smoke test → http verify → clickbot → clickbot report
  → save deploy state
  → Message End Event: publish "msg_deploy_done" [trigger_sha]

Error Event Subprocess:
  → create Odoo error task → rollback
  → Message Intermediate Throw: publish "msg_deploy_done" [trigger_sha] (deploy_failed=true)
  → Error End Event: throw DEPLOY_FAILED (propagates to Call Activity parent, if any)
```

Note: deploy flow order is module-update → docker-up → cache-clear (not docker-up first), matching the current BPMN sequence flows.

**Output variables in `msg_deploy_done`:**
- `smoke_passed` (bool)
- `clickbot_passed` (bool)
- `clickbot_report` (str)
- `modules_updated` (str)
- `deploy_failed` (bool, only on error)
- `error_message` (str, only on error)

**Input variables from `msg_deploy_trigger`:**
- `trigger_sha` (str) — correlation key
- `server_host` (str) — e.g. "staging"
- `ssh_user` (str)
- `repo_dir` (str)
- `db_name` (str)
- `container` (str)
- `branch` (str) — e.g. "staging"
- `run_smoke_test` (bool)
- `test_mode` (str)
- `parent_process_instance_key` (int, optional — present when pipeline is running, absent for standalone)
- `odoo_project_id` (int)

When `parent_process_instance_key` is absent (standalone deploy), Odoo tasks (clickbot report, error notification) are created without linking to a parent pipeline.

### Verification Process (`verification-process.bpmn`)

**No changes.** Remains a Call Activity — no external trigger (only human verification through Odoo tasks).

### Pipeline (`feature-to-production.bpmn`)

**Becomes a pure orchestrator.** Call Activities for review and deploy replaced with Message Catch Events.

**Flow:**
```
Start Event (manual, /start)
  → XOR: odoo_task_id exists?
      ├── Yes → merge
      └── No  → create Odoo task (http-request-smart) → merge
  → Message Catch "msg_pr_event" [correlation=head_branch]   -- wait for Draft PR

  → Message Catch "msg_review_done" [correlation=pr_number]   -- wait for review result
  → XOR: score >= 7?
      ├── Yes → continue
      └── No  → comment "потребує виправлення" (github-comment)
               → Receive Task: wait "msg_pr_updated" [pr_number]
                   ├── Boundary Timer (R/P3D, non-interrupting) → reminder
               → loop back to Message Catch "msg_review_done"

  → merge feature → staging (Service Task, job: merge-feature-to-staging)
      outputs: merge_sha
  → Message Catch "msg_deploy_done" [correlation=merge_sha]    -- wait for deploy result
  → XOR: deploy OK?
      ├── Yes → continue
      └── No  → rework path

  → Call Activity verification-process                          -- staging verify (human)
  → XOR: staging approved?
      ├── Yes → continue
      └── No  → rework path

  → undraft PR (Service Task, job: github-pr-ready)
  → Call Activity verification-process                          -- merge verify (human)
  → XOR: merge approved?
      ├── Yes → continue
      └── No  → rework path

  → github-merge (Service Task, job: github-merge)
  → Call Activity verification-process                          -- prod deploy (human)
  → End

Rework path:
  → comment "потрібне виправлення" (github-comment)
  → Receive Task: wait "msg_pr_updated" [pr_number]
  → loop back to Message Catch "msg_review_done"

Error Event Subprocess:
  → notify Odoo (http-request-smart)
  → End
```

**Key changes:**
- `call_pr_review` (Call Activity) → Message Catch `msg_review_done` [pr_number]
- `call_pr_review_2nd` (Call Activity) → removed (latest review result is sufficient)
- `call_deploy_staging` (Call Activity) → Message Catch `msg_deploy_done` [merge_sha]
- Rework loop adapted: after msg_pr_updated, review auto-restarts from webhook, pipeline waits for new msg_review_done
- verification-process remains Call Activity (no external trigger)

### Webhook Server (`webhook.py`)

**New handler for push events:**

```python
# POST /webhook/github

# Existing (unchanged):
event=pull_request, action=opened/reopened, base=main
  → publish msg_pr_event [correlation=head_branch]
    variables: pr_number, pr_url, pr_title, head_branch, repository, ...

event=pull_request, action=synchronize
  → publish msg_pr_updated [correlation=pr_number]
    variables: pr_updated=True, head_sha

# New:
event=push, ref=refs/heads/staging
  → publish msg_deploy_trigger [correlation=after_sha]
    variables: trigger_sha=after, server_host="staging",
    ssh_user, repo_dir, db_name, container, branch="staging",
    run_smoke_test=True, test_mode="full",
    odoo_project_id
```

**Filtering rules:**
- Push events: only `refs/heads/staging` triggers deploy. All other branches ignored.
- PR events: only PRs targeting `main` trigger review (unchanged).
- Server config for staging injected from `AppConfig.servers["staging"]`.
- Push event payload structure differs from PR events: uses `ref`, `before`, `after`, `head_commit` (no `pull_request` key).

### Main-to-Staging Sync (`main-to-staging-sync.bpmn`)

**Simplified:** remove Call Activity deploy-process. Deploy triggers automatically from push event.

```
Timer Start (nightly, R/P1D)
  → merge main → staging (Service Task, job: merge-to-staging)
  → End

Boundary Error on merge → notify Odoo → End
```

### Upstream Sync (`upstream-sync.bpmn`)

**Partial change:** remove `call_deploy_staging` (deploy triggers from push). Keep `call_deploy_demo` (demo deploy has no push trigger).

```
... → sync → audit → commit+push
  → deploy demo (Call Activity deploy-process, unchanged — uses None Start Event)
  → review task → PR → merge to staging
  → End  (deploy triggers automatically from push to staging)
```

### Message Catalog

| Message name | Publisher | Correlation key | Consumer | TTL |
|---|---|---|---|---|
| `msg_pr_event` | webhook (PR opened/reopened) | `head_branch` | pr-review (Message Start), pipeline (Message Catch) | 1h |
| `msg_pr_updated` | webhook (PR synchronize) | `pr_number` (string) | pr-review (Receive Task) | 1h |
| `msg_deploy_trigger` | webhook (push staging) | `trigger_sha` | deploy-process (Message Start) | 1h |
| `msg_review_done` | pr-review (Message End) | `pr_number` (string) | pipeline (Message Catch) | 1h |
| `msg_deploy_done` | deploy-process (Message End / Error Subprocess) | `trigger_sha` | pipeline (Message Catch) | 1h |
| `msg_odoo_task_done` | webhook (Odoo callback) | `process_instance_key` (string) | verification-process (Receive Task) | 1h |

**TTL implementation:** All messages published via `zeebe_client.publish_message()` with `time_to_live=3_600_000` (1 hour in milliseconds). This covers the maximum deploy duration (clickbot timeout = 1 hour) plus buffer for pipeline to open its Message Catch.

### Error Handling

**Deploy failure (dual-path error handling):**

The Error Event Subprocess in deploy-process handles both invocation modes:
1. Publishes `msg_deploy_done` with `deploy_failed=true` (Message Intermediate Throw Event) — consumed by pipeline if listening
2. Throws BPMN Error `DEPLOY_FAILED` via Error End Event — caught by Call Activity parent (upstream-sync demo deploy)

When standalone (no Call Activity parent): the Error End Event terminates the process instance with error state. The message was already published before the Error End Event, so the pipeline (if listening) receives the failure notification.

When called via Call Activity (demo deploy): the calling process catches `DEPLOY_FAILED` as before. The published `msg_deploy_done` message expires (no pipeline listening for demo deploys).

**Review — no special error handling needed.** PR-Agent failure uses existing Zeebe retry mechanism. If all retries exhausted, worker exception handler throws BPMN Error, caught by error event subprocess.

**Race condition (message published before pipeline listens):** Zeebe message buffer with 1-hour TTL. Pipeline opens Message Catch → receives buffered message immediately.

**Concurrent deploys on same server:**

Two deploy instances may start from near-simultaneous pushes (e.g., feature merge + main sync). While Zeebe creates separate process instances, the actual SSH commands execute on the same staging server filesystem.

Mitigation: Zeebe job worker processes one job at a time per task type (pyzeebe default `max_jobs_to_activate=32` but executes sequentially within `worker_loop`). Since all deploy tasks (git-pull, docker-build, module-update, etc.) go through the same single-threaded worker, deploy A's tasks complete before deploy B's tasks of the same type start. Combined with the sequential nature of the deploy flow (git-pull → detect → build → update → ...), interleaving is unlikely in practice.

For additional safety, the `git-pull` handler reads `deploy_state` file to detect stale state, and `detect-modules` compares commit SHAs — a concurrent deploy would see "no changes" if the first deploy already pulled the same code.

**Standalone deploy (no pipeline listening):** `msg_deploy_done` expires after TTL. No harm.

### GitHub Configuration Required

In GitHub repo `tut-ua/odoo-enterprise` → Settings → Webhooks → Edit existing webhook:

Add event:
- [x] **Pushes** — triggers deploy on staging merge

Existing events (keep):
- [x] **Pull requests** — triggers review on PR open, rework on synchronize

No changes to webhook URL, secret, or content type.

## Files to Modify

| File | Change |
|---|---|
| `bpmn/ci-cd/pr-review.bpmn` | None Start → Message Start; add Message End Event |
| `bpmn/ci-cd/deploy-process.bpmn` | Add Message Start (keep None Start for demo); add Message End; Error Subprocess publishes message AND throws Error |
| `bpmn/ci-cd/feature-to-production.bpmn` | Replace Call Activities with Message Catch Events; remove 2nd review; simplify rework loop |
| `bpmn/ci-cd/main-to-staging-sync.bpmn` | Remove Call Activity deploy-process |
| `bpmn/ci-cd/upstream-sync.bpmn` | Remove call_deploy_staging Call Activity (keep call_deploy_demo) |
| `worker/webhook.py` | Add push event handler for staging branch |
| `worker/handlers/sync.py` | `merge-feature-to-staging` handler: add `merge_sha` to output (run `git rev-parse HEAD` after push, return SHA) |
| Tests | Update integration and unit tests for new message flow |

## Out of Scope

- Production deploy automation (remains manual via verification-process)
- Changes to verification-process (no external trigger)
- Changes to deploy worker handlers other than `merge-feature-to-staging` (Python code unchanged)
- Changes to review worker handlers (Python code unchanged)
- Webhook for push to branches other than staging

## Correlation Key: merge_sha ↔ trigger_sha

The pipeline correlation between `merge_sha` and `trigger_sha` depends on both values being the same commit SHA:

1. `merge-feature-to-staging` handler merges the feature branch into staging, pushes, and runs `git rev-parse HEAD` → returns `merge_sha`
2. GitHub push webhook fires with `after` = HEAD SHA of the staging branch after push → `trigger_sha = after`
3. These are the same SHA — the HEAD of staging after the merge+push operation

This is reliable because the push is atomic: the handler pushes a specific merge commit, and GitHub's push event reports that exact commit as `after`. No other push can change staging HEAD between the handler's push and the webhook event, because the webhook fires synchronously from GitHub's perspective.
