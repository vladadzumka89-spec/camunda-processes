# Event-Driven CI/CD Sub-processes Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make pr-review and deploy-process event-driven (triggered by GitHub webhooks), transforming feature-to-production into a pure orchestrator.

**Architecture:** Sub-processes start from Message Start Events (GitHub webhooks) and publish result messages at the end. Pipeline waits for results via Message Catch Events instead of Call Activities. Deploy-process keeps both None Start (for Call Activity from upstream-sync demo) and Message Start (for webhook triggers).

**Tech Stack:** Camunda 8.8 Zeebe, Python 3.12 async, pyzeebe, aiohttp, BPMN XML

**Spec:** `docs/superpowers/specs/2026-03-15-event-driven-cicd-design.md`

---

## Chunk 1: Worker changes (webhook + merge handler)

### Task 1: Add `merge_sha` output to `merge-feature-to-staging` handler

**Files:**
- Modify: `worker/handlers/sync.py:758-773`
- Test: `tests/test_handlers_sync.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_handlers_sync.py`:

```python
@pytest.mark.asyncio
async def test_merge_feature_to_staging_returns_merge_sha(
    handlers: dict, mock_ssh: AsyncMock,
) -> None:
    """merge-feature-to-staging should return merge_sha (HEAD after push)."""
    mock_ssh.run.side_effect = [
        _make_ssh_result(),                    # git clone
        _make_ssh_result(),                    # git fetch
        _make_ssh_result(exit_code=0),         # git merge
        _make_ssh_result(),                    # git push
        _make_ssh_result(stdout="abc123\n"),   # git rev-parse HEAD
        _make_ssh_result(),                    # rm -rf workspace
    ]
    job = _make_mock_job(99999)
    result = await handlers["merge-feature-to-staging"](
        job=job,
        feature_branch="feat/x",
        server_host="staging",
    )
    assert result["staging_merged"] is True
    assert result["merge_sha"] == "abc123"
    # Verify rev-parse was called
    calls = mock_ssh.run.call_args_list
    assert "rev-parse HEAD" in calls[4].args[1]
```

Uses existing test helpers: `_make_ssh_result()`, `_make_mock_job()`, `handlers` fixture, matching the pattern in `test_merge_feature_to_staging_success` (line 229).

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_handlers_sync.py::test_merge_feature_to_staging_returns_merge_sha -v`
Expected: FAIL — `merge_sha` not in result

- [ ] **Step 3: Implement — add `git rev-parse HEAD` after push**

In `worker/handlers/sync.py`:

1. Initialize `merge_sha` before the `try` block (line 715) to avoid `UnboundLocalError`:
```python
        merge_sha = ""
        try:
```

2. After the push (line 763), before `logger.info` (line 765), add:
```python
            # Get merge commit SHA for correlation with deploy process
            sha_result = await ssh.run(
                server,
                f"cd {workspace} && git rev-parse HEAD",
                check=True, timeout=15,
            )
            merge_sha = sha_result.stdout.strip()
```

3. Change the return (line 770-773) to:
```python
        return {
            "staging_merged": True,
            "merge_sha": merge_sha,
            "process_instance_key": job.process_instance_key,
        }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_handlers_sync.py::test_merge_feature_to_staging_returns_merge_sha -v`
Expected: PASS

- [ ] **Step 5: Run all sync tests to check nothing broke**

Run: `pytest tests/test_handlers_sync.py -v`
Expected: All pass

---

### Task 2: Add push event handler to webhook server

**Files:**
- Modify: `worker/webhook.py:96-128`
- Test: `tests/test_webhook.py`

- [ ] **Step 1: Write the failing test for push event routing**

Add to `tests/test_webhook.py`:

```python
@pytest.mark.asyncio
async def test_push_staging_publishes_deploy_trigger(
    client: TestClient, app_config: AppConfig,
) -> None:
    """Push to staging branch should publish msg_deploy_trigger."""
    payload = {
        "ref": "refs/heads/staging",
        "after": "abc123def456",
        "before": "000000",
        "repository": {"full_name": "tut-ua/odoo-enterprise"},
        "head_commit": {"id": "abc123def456", "message": "Merge feat/x"},
        "pusher": {"name": "deploy-bot"},
    }
    body = json.dumps(payload).encode()
    sig = _sign(body, app_config.github.webhook_secret)

    with patch.object(WebhookServer, "_create_zeebe_client") as mock_factory:
        mock_client = AsyncMock()
        mock_client.publish_message = AsyncMock()
        mock_factory.return_value = mock_client

        resp = await client.post(
            "/webhook/github",
            data=body,
            headers={
                "X-GitHub-Event": "push",
                "X-Hub-Signature-256": sig,
                "Content-Type": "application/json",
            },
        )
        assert resp.status == 200
        data = await resp.json()
        assert data["message"] == "msg_deploy_trigger"

        call_kwargs = mock_client.publish_message.call_args[1]
        assert call_kwargs["name"] == "msg_deploy_trigger"
        assert call_kwargs["correlation_key"] == "abc123def456"
        assert call_kwargs["variables"]["trigger_sha"] == "abc123def456"
        assert call_kwargs["variables"]["server_host"] == "staging.example.com"
        assert call_kwargs["variables"]["branch"] == "staging"
```

- [ ] **Step 2: Write test for push to non-staging branch (should ignore)**

```python
@pytest.mark.asyncio
async def test_push_non_staging_ignored(
    client: TestClient, app_config: AppConfig,
) -> None:
    """Push to non-staging branch should be ignored."""
    payload = {
        "ref": "refs/heads/main",
        "after": "abc123",
        "repository": {"full_name": "tut-ua/odoo-enterprise"},
    }
    body = json.dumps(payload).encode()
    sig = _sign(body, app_config.github.webhook_secret)

    resp = await client.post(
        "/webhook/github",
        data=body,
        headers={
            "X-GitHub-Event": "push",
            "X-Hub-Signature-256": sig,
            "Content-Type": "application/json",
        },
    )
    assert resp.status == 200
    data = await resp.json()
    assert data["status"] == "ignored"
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `pytest tests/test_webhook.py::test_push_staging_publishes_deploy_trigger tests/test_webhook.py::test_push_non_staging_ignored -v`
Expected: FAIL

- [ ] **Step 4: Implement push event routing in webhook.py**

In `worker/webhook.py`, modify `_handle_github` (line 123-128) to route push events:

```python
        # 3. Route by event type
        if event_type == 'pull_request':
            return await self._route_pr_event(payload)
        elif event_type == 'push':
            return await self._route_push_event(payload)

        # Ignore other events
        return web.json_response({"status": "ignored", "event": event_type})
```

Add the new method after `_route_pr_event`:

```python
    async def _route_push_event(self, payload: dict) -> web.Response:
        """Route push events — deploy staging on push to staging branch."""
        ref = payload.get('ref', '')
        after_sha = payload.get('after', '')

        if ref != 'refs/heads/staging':
            logger.info("Ignoring push to %s (not staging)", ref)
            return web.json_response({"status": "ignored", "ref": ref})

        staging = self._config.servers.get('staging')
        if not staging:
            logger.error("No staging server configured for deploy trigger")
            return web.Response(status=500, text="No staging server configured")

        variables: dict[str, Any] = {
            "trigger_sha": after_sha,
            "server_host": staging.host,
            "ssh_user": staging.ssh_user,
            "repo_dir": staging.repo_dir,
            "db_name": staging.db_name,
            "container": staging.container,
            "branch": "staging",
            "run_smoke_test": True,
            "test_mode": "full",
            "odoo_project_id": self._config.odoo.project_id,
        }

        try:
            client = self._create_zeebe_client()
            await client.publish_message(
                name="msg_deploy_trigger",
                correlation_key=after_sha,
                variables=variables,
                time_to_live=3_600_000,
            )
            logger.info(
                "Published msg_deploy_trigger for push to staging (sha=%s)",
                after_sha[:12],
            )
            return web.json_response({
                "status": "published",
                "message": "msg_deploy_trigger",
                "trigger_sha": after_sha,
            })
        except Exception as exc:
            logger.error("Failed to publish msg_deploy_trigger: %s", exc)
            return web.Response(status=502, text=f"Zeebe publish failed: {exc}")
```

Update module docstring (line 1-12) to mention the new message:
```
    msg_deploy_trigger — push to staging → starts deploy-process
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/test_webhook.py -v`
Expected: All pass (including new tests)

- [ ] **Step 6: Also add TTL to existing publish_message calls**

Add `time_to_live=3_600_000` to `_publish_pr_event` (line 197) and `_publish_pr_updated` (line 221). This ensures messages don't pile up if no one consumes them.

- [ ] **Step 7: Run full test suite**

Run: `pytest tests/test_webhook.py -v`
Expected: All pass

---

## Chunk 2: BPMN changes — pr-review and deploy-process

### Task 3: Update pr-review.bpmn — Message Start + Message End

**Files:**
- Modify: `bpmn/ci-cd/pr-review.bpmn`

- [ ] **Step 1: Add Message definition for msg_pr_event**

After the existing `Message_pr_updated` definition (line 6-10), add:

```xml
  <bpmn:message id="Message_pr_event" name="msg_pr_event">
    <bpmn:extensionElements>
      <zeebe:subscription correlationKey="=head_branch" />
    </bpmn:extensionElements>
  </bpmn:message>
```

- [ ] **Step 2: Change Start Event to Message Start Event**

Replace the None Start Event `evt_start` (line 18-20) with a Message Start Event:

```xml
    <bpmn:startEvent id="evt_start" name="PR відкрито">
      <bpmn:outgoing>f_start_to_review</bpmn:outgoing>
      <bpmn:messageEventDefinition id="MsgDef_start" messageRef="Message_pr_event" />
    </bpmn:startEvent>
```

- [ ] **Step 3: Add Message definition for msg_review_done**

Add after the other messages:

```xml
  <bpmn:message id="Message_review_done" name="msg_review_done" />
```

- [ ] **Step 4: Write tests for `publish-message` handler (test-first)**

**Before implementing**, write the tests. Create `tests/test_handlers_messaging.py`:

```python
"""Tests for publish-message handler."""
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from worker.config import AppConfig


def _extract_handler(app_config):
    from worker.handlers.messaging import register_messaging_handlers
    handlers = {}
    mock_worker = MagicMock()
    def capture(task_type, timeout_ms=30000):
        def decorator(fn):
            handlers[task_type] = fn
            return fn
        return decorator
    mock_worker.task = capture
    register_messaging_handlers(mock_worker, app_config)
    return handlers["publish-message"]


@pytest.mark.asyncio
async def test_publish_message_success(app_config):
    handler = _extract_handler(app_config)
    job = MagicMock()
    job.variables = {"review_score": 8, "has_critical_issues": False}

    with patch("worker.handlers.messaging.create_channel"), \
         patch("worker.handlers.messaging.ZeebeClient") as MockClient:
        mock_instance = AsyncMock()
        MockClient.return_value = mock_instance

        result = await handler(
            job,
            message_name="msg_review_done",
            correlation_key="42",
        )

    assert result["message_published"] is True
    mock_instance.publish_message.assert_awaited_once()
    call_kw = mock_instance.publish_message.call_args[1]
    assert call_kw["name"] == "msg_review_done"
    assert call_kw["correlation_key"] == "42"
    assert call_kw["time_to_live"] == 3_600_000


@pytest.mark.asyncio
async def test_publish_message_missing_name(app_config):
    handler = _extract_handler(app_config)
    job = MagicMock()
    job.variables = {}
    with pytest.raises(ValueError, match="message_name"):
        await handler(job, message_name="", correlation_key="42")


@pytest.mark.asyncio
async def test_publish_message_missing_correlation(app_config):
    handler = _extract_handler(app_config)
    job = MagicMock()
    job.variables = {}
    with pytest.raises(ValueError, match="correlation_key"):
        await handler(job, message_name="test", correlation_key="")
```

- [ ] **Step 5: Implement `publish-message` handler**

Zeebe 8.8 Message Throw/End Events are complex to configure correctly. The cleanest approach: a reusable `publish-message` Service Task that publishes a Zeebe message via the worker. Used by both pr-review and deploy-process.

Create a new handler that publishes a Zeebe message. This will be used by both pr-review and deploy-process to publish their result messages.

Add to `worker/handlers/messaging.py`:

```python
"""Message publishing handler — publishes Zeebe messages from BPMN processes."""

import logging
from pyzeebe import Job, ZeebeWorker, ZeebeClient
from ..auth import ZeebeAuthConfig, create_channel
from ..config import AppConfig

logger = logging.getLogger(__name__)

def register_messaging_handlers(worker: ZeebeWorker, config: AppConfig) -> None:
    """Register message publishing task handlers."""

    def _create_client() -> ZeebeClient:
        auth_config = ZeebeAuthConfig(
            gateway_address=config.zeebe.gateway_address,
            client_id=config.zeebe.client_id,
            client_secret=config.zeebe.client_secret,
            token_url=config.zeebe.token_url,
            audience=config.zeebe.audience,
            use_tls=config.zeebe.use_tls,
        )
        return ZeebeClient(create_channel(auth_config))

    @worker.task(task_type="publish-message", timeout_ms=30_000)
    async def publish_message(
        job: Job,
        message_name: str = "",
        correlation_key: str = "",
        **kwargs,
    ) -> dict:
        """Publish a Zeebe message with all process variables as payload."""
        if not message_name:
            raise ValueError("message_name is required")
        if not correlation_key:
            raise ValueError("correlation_key is required")

        client = _create_client()
        await client.publish_message(
            name=message_name,
            correlation_key=str(correlation_key),
            variables=dict(job.variables),
            time_to_live=3_600_000,
        )
        logger.info(
            "Published message %s (correlation=%s)",
            message_name, correlation_key,
        )
        return {"message_published": True}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `pytest tests/test_handlers_messaging.py -v`
Expected: All 3 pass

- [ ] **Step 7: Register the handler in `worker/handlers/__init__.py`**

Add import and registration call.

- [ ] **Step 8: Use `publish-message` Service Task in pr-review.bpmn before End**

Replace the End Event section with:

```xml
    <bpmn:serviceTask id="ST_publish_review_done" name="Публікувати результат ревʼю">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="publish-message" retries="2" />
        <zeebe:ioMapping>
          <zeebe:input source="=&quot;msg_review_done&quot;" target="message_name" />
          <zeebe:input source="=string(pr_number)" target="correlation_key" />
        </zeebe:ioMapping>
      </bpmn:extensionElements>
      <bpmn:incoming>f_score_ok</bpmn:incoming>
      <bpmn:outgoing>f_publish_to_end</bpmn:outgoing>
    </bpmn:serviceTask>

    <bpmn:endEvent id="evt_end" name="Ревʼю пройдено">
      <bpmn:incoming>f_publish_to_end</bpmn:incoming>
    </bpmn:endEvent>

    <bpmn:sequenceFlow id="f_publish_to_end" sourceRef="ST_publish_review_done" targetRef="evt_end" />
```

Update `f_score_ok` to target `ST_publish_review_done` instead of `evt_end`.

- [ ] **Step 9: Update versionTag to 2.0**

Change `<zeebe:versionTag value="1.0" />` to `<zeebe:versionTag value="2.0" />`.

- [ ] **Step 10: Run layout checker**

Run: `python3 bpmn_layout_checker.py bpmn/ci-cd/pr-review.bpmn`

- [ ] **Step 11: Run auto-layout if needed**

Run: `python3 bpmn_auto_layout.py bpmn/ci-cd/pr-review.bpmn`

---

### Task 4: Update deploy-process.bpmn — Message Start + Message End + Error Subprocess

**Files:**
- Modify: `bpmn/ci-cd/deploy-process.bpmn`

- [ ] **Step 1: Add Message definitions**

After the error definition (line 25), add:

```xml
  <bpmn:message id="Message_deploy_trigger" name="msg_deploy_trigger">
    <bpmn:extensionElements>
      <zeebe:subscription correlationKey="=trigger_sha" />
    </bpmn:extensionElements>
  </bpmn:message>
```

- [ ] **Step 2: Add Message Start Event (keep existing None Start)**

After `evt_start` (None Start, line 33-35), add a new Message Start Event:

```xml
    <bpmn:startEvent id="evt_start_msg" name="Deploy triggered (webhook)">
      <bpmn:outgoing>f01_msg</bpmn:outgoing>
      <bpmn:messageEventDefinition id="MsgDef_deploy_trigger" messageRef="Message_deploy_trigger" />
    </bpmn:startEvent>
```

Add a merge gateway after both starts, before `task_git_pull`:

```xml
    <bpmn:exclusiveGateway id="gw_merge_start">
      <bpmn:incoming>f01</bpmn:incoming>
      <bpmn:incoming>f01_msg</bpmn:incoming>
      <bpmn:outgoing>f01_merged</bpmn:outgoing>
    </bpmn:exclusiveGateway>
```

Update `task_git_pull` incoming from `f01` to `f01_merged`.

Add flows:
```xml
    <bpmn:sequenceFlow id="f01_msg" sourceRef="evt_start_msg" targetRef="gw_merge_start" />
    <bpmn:sequenceFlow id="f01_merged" sourceRef="gw_merge_start" targetRef="task_git_pull" />
```

Update `f01` target from `task_git_pull` to `gw_merge_start`.

- [ ] **Step 3: Add `publish-message` Service Task before End Success**

Insert between `task_save_state` and `evt_end_success`:

```xml
    <bpmn:serviceTask id="ST_publish_deploy_done" name="Публікувати результат деплою">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="publish-message" retries="2" />
        <zeebe:ioMapping>
          <zeebe:input source="=&quot;msg_deploy_done&quot;" target="message_name" />
          <zeebe:input source="=if is defined(trigger_sha) and trigger_sha != null then trigger_sha else &quot;none&quot;" target="correlation_key" />
        </zeebe:ioMapping>
      </bpmn:extensionElements>
      <bpmn:incoming>f18</bpmn:incoming>
      <bpmn:outgoing>f_publish_to_end</bpmn:outgoing>
    </bpmn:serviceTask>
```

Update `f18` to target `ST_publish_deploy_done` (instead of `evt_end_success`).
Add: `<bpmn:sequenceFlow id="f_publish_to_end" sourceRef="ST_publish_deploy_done" targetRef="evt_end_success" />`

The `trigger_sha` check handles Call Activity path (no `trigger_sha` variable) — publishes with "none" correlation (nobody catches it, expires).

- [ ] **Step 4: Update Error Event Subprocess — publish message before throwing error**

In `subprocess_error` (line 216-259), insert a message publish task between `task_rollback` and `evt_error_end`:

```xml
      <bpmn:serviceTask id="ST_publish_deploy_failed" name="Публікувати помилку деплою">
        <bpmn:extensionElements>
          <zeebe:taskDefinition type="publish-message" retries="1" />
          <zeebe:ioMapping>
            <zeebe:input source="=&quot;msg_deploy_done&quot;" target="message_name" />
            <zeebe:input source="=if is defined(trigger_sha) and trigger_sha != null then trigger_sha else &quot;none&quot;" target="correlation_key" />
            <zeebe:input source="=true" target="deploy_failed" />
            <zeebe:input source="=if is defined(caught_error_message) and caught_error_message != null then caught_error_message else &quot;Unknown error&quot;" target="error_message" />
          </zeebe:ioMapping>
        </bpmn:extensionElements>
        <bpmn:incoming>f_err2</bpmn:incoming>
        <bpmn:outgoing>f_err3</bpmn:outgoing>
      </bpmn:serviceTask>
```

Update flow: `task_rollback` → `ST_publish_deploy_failed` → `evt_error_end`.
- `f_err2`: sourceRef=task_rollback targetRef=ST_publish_deploy_failed
- New: `f_err3`: sourceRef=ST_publish_deploy_failed targetRef=evt_error_end

- [ ] **Step 5: Also add publish for "no changes" early exit**

The `evt_end_no_changes` (line 56-58) should also publish `msg_deploy_done` with `no_changes=true`. Add a Service Task before it:

```xml
    <bpmn:serviceTask id="ST_publish_no_changes" name="Публікувати: без змін">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="publish-message" retries="2" />
        <zeebe:ioMapping>
          <zeebe:input source="=&quot;msg_deploy_done&quot;" target="message_name" />
          <zeebe:input source="=if is defined(trigger_sha) and trigger_sha != null then trigger_sha else &quot;none&quot;" target="correlation_key" />
          <zeebe:input source="=true" target="no_changes" />
        </zeebe:ioMapping>
      </bpmn:extensionElements>
      <bpmn:incoming>f03</bpmn:incoming>
      <bpmn:outgoing>f03_to_end</bpmn:outgoing>
    </bpmn:serviceTask>
```

Update `f03` to target `ST_publish_no_changes`. Add `f03_to_end` → `evt_end_no_changes`.

- [ ] **Step 6: Guard `parent_process_instance_key` in FEEL expressions**

The existing `ST_clickbot_report` (line 193) and `ST_error_odoo_task` (line 234) reference `parent_process_instance_key` in FEEL without checking if it's defined. In standalone mode (webhook trigger), this variable is absent and FEEL will fail.

Update both FEEL body expressions to guard:
- Replace `parent_process_instance_key: parent_process_instance_key` with:
  `parent_process_instance_key: if is defined(parent_process_instance_key) and parent_process_instance_key != null then parent_process_instance_key else process_instance_key`

This falls back to the deploy process's own instance key when no parent is set.

- [ ] **Step 7: Update versionTag to 2.0**

- [ ] **Step 8: Run layout checker and auto-layout**

Run: `python3 bpmn_layout_checker.py bpmn/ci-cd/deploy-process.bpmn`
Run: `python3 bpmn_auto_layout.py bpmn/ci-cd/deploy-process.bpmn`

---

## Chunk 3: BPMN changes — feature-to-production pipeline

### Task 5: Rewrite feature-to-production.bpmn as pure orchestrator

**Files:**
- Modify: `bpmn/ci-cd/feature-to-production.bpmn`

This is the largest change. The pipeline replaces Call Activities with Message Catch Events.

- [ ] **Step 1: Add new Message definitions**

Add after existing messages (line 36-45):

```xml
  <bpmn:message id="Message_review_done" name="msg_review_done">
    <bpmn:extensionElements>
      <zeebe:subscription correlationKey="=string(pr_number)" />
    </bpmn:extensionElements>
  </bpmn:message>
  <bpmn:message id="Message_deploy_done" name="msg_deploy_done">
    <bpmn:extensionElements>
      <zeebe:subscription correlationKey="=merge_sha" />
    </bpmn:extensionElements>
  </bpmn:message>
```

- [ ] **Step 2: Replace `call_pr_review` with Message Catch Event**

Remove the Call Activity `call_pr_review` (lines 97-112).

Replace with:

```xml
    <bpmn:intermediateCatchEvent id="catch_review_done" name="Чекаємо результат ревʼю">
      <bpmn:documentation>Чекає msg_review_done від standalone pr-review процесу.</bpmn:documentation>
      <bpmn:incoming>f_catch_to_review</bpmn:incoming>
      <bpmn:incoming>f_rework_catch_to_review</bpmn:incoming>
      <bpmn:outgoing>f_review_to_gw_score</bpmn:outgoing>
      <bpmn:messageEventDefinition id="MsgDef_catch_review" messageRef="Message_review_done" />
    </bpmn:intermediateCatchEvent>
```

- [ ] **Step 3: Add XOR gateway for score check (was inside pr-review, now in pipeline)**

The pr-review process still loops internally until score >= 7, so the pipeline actually just receives the "passed" result. However, to be safe and handle edge cases, keep a score check:

```xml
    <bpmn:exclusiveGateway id="gw_review_score" name="Score >= 7?" default="f_review_ok">
      <bpmn:incoming>f_review_to_gw_score</bpmn:incoming>
      <bpmn:outgoing>f_review_ok</bpmn:outgoing>
      <bpmn:outgoing>f_review_low</bpmn:outgoing>
    </bpmn:exclusiveGateway>
```

With condition on `f_review_low`:
```xml
    <bpmn:sequenceFlow id="f_review_low" name="Score low" sourceRef="gw_review_score" targetRef="task_comment_rework">
      <bpmn:conditionExpression xsi:type="bpmn:tFormalExpression">=review_score &lt; 7 or has_critical_issues = true</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
```

Actually, since pr-review only publishes `msg_review_done` when score >= 7, the pipeline will always get a passing result. The score check is redundant. But keep it as a safety net — if the sub-process behavior changes.

Connect: `f_review_ok` → `task_merge_staging`.

- [ ] **Step 4: Add `merge_sha` output mapping to merge task**

Update `task_merge_staging` io mapping to capture `merge_sha`:

```xml
        <zeebe:ioMapping>
          <zeebe:input source="=head_branch" target="feature_branch" />
          <zeebe:input source="=staging_host" target="server_host" />
          <zeebe:input source="=repository" target="repository" />
          <zeebe:output source="=merge_sha" target="merge_sha" />
        </zeebe:ioMapping>
```

- [ ] **Step 5: Replace `call_deploy_staging` with Message Catch Event**

Remove the Call Activity `call_deploy_staging` (lines 149-172) and its boundary error event.

Replace with:

```xml
    <bpmn:intermediateCatchEvent id="catch_deploy_done" name="Чекаємо результат деплою">
      <bpmn:documentation>Чекає msg_deploy_done від standalone deploy-process (triggered by push to staging).</bpmn:documentation>
      <bpmn:incoming>f_merge_to_deploy_staging</bpmn:incoming>
      <bpmn:outgoing>f_deploy_to_gw</bpmn:outgoing>
      <bpmn:messageEventDefinition id="MsgDef_catch_deploy" messageRef="Message_deploy_done" />
    </bpmn:intermediateCatchEvent>
```

- [ ] **Step 6: Add XOR gateway for deploy result**

```xml
    <bpmn:exclusiveGateway id="gw_deploy_ok" name="Deploy OK?" default="f_deploy_ok">
      <bpmn:incoming>f_deploy_to_gw</bpmn:incoming>
      <bpmn:outgoing>f_deploy_ok</bpmn:outgoing>
      <bpmn:outgoing>f_deploy_failed</bpmn:outgoing>
    </bpmn:exclusiveGateway>
```

With condition:
```xml
    <bpmn:sequenceFlow id="f_deploy_failed" name="Failed" sourceRef="gw_deploy_ok" targetRef="task_notify_deploy_fail">
      <bpmn:conditionExpression xsi:type="bpmn:tFormalExpression">=is defined(deploy_failed) and deploy_failed = true</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
```

Connect `f_deploy_ok` → `call_verify_staging`.

- [ ] **Step 7: Remove 2nd PR review Call Activity**

Remove `call_pr_review_2nd` (lines 230-244). Connect `gw_staging_ok` OK flow directly to `task_undraft_pr`.

Update: `f_staging_ok` → `task_undraft_pr` (instead of `call_pr_review_2nd`).
Remove: `f_2nd_review_to_undraft`.

- [ ] **Step 8: Update all sequence flows**

Rewire all flows to match the new structure. Key changes:
- `f_catch_to_review` → targets `catch_review_done` (was `call_pr_review`)
- `f_review_to_merge_staging` renamed to `f_review_ok` → targets `task_merge_staging`
- `f_merge_to_deploy_staging` → targets `catch_deploy_done` (was `call_deploy_staging`)
- `f_deploy_to_verify_staging` renamed to `f_deploy_ok` → targets `call_verify_staging`
- `f_staging_ok` → targets `task_undraft_pr` directly
- `f_rework_catch_to_review` → targets `catch_review_done`

- [ ] **Step 9: Update versionTag to 6.0**

- [ ] **Step 10: Run layout checker and auto-layout**

Run: `python3 bpmn_layout_checker.py bpmn/ci-cd/feature-to-production.bpmn`
Run: `python3 bpmn_auto_layout.py bpmn/ci-cd/feature-to-production.bpmn`

---

## Chunk 4: Simplify dependent processes + tests + deploy

### Task 6: Simplify main-to-staging-sync.bpmn

**Files:**
- Modify: `bpmn/ci-cd/main-to-staging-sync.bpmn`

- [ ] **Step 1: Remove Call Activity and deploy error boundary**

Remove `call_deploy_staging` (lines 62-80), `evt_deploy_error` (lines 83-86), and `f_deploy_error` flow (line 118).

- [ ] **Step 2: Connect merge directly to End**

Change `f_merge_to_deploy` to: `sourceRef="task_merge_main_staging" targetRef="evt_end_success"`.

Remove `f_deploy_to_end`, `f_deploy_error`.

Update `task_notify_fail` to only have `f_merge_error` as incoming (remove `f_deploy_error`).

- [ ] **Step 3: Update versionTag to 2.0**

- [ ] **Step 4: Run layout checker**

---

### Task 7: Update upstream-sync.bpmn — remove call_deploy_staging

**Files:**
- Modify: `bpmn/ci-cd/upstream-sync.bpmn`

- [ ] **Step 1: Remove `call_deploy_staging` Call Activity**

Remove the staging deploy Call Activity and connect `task_merge_staging` directly to `evt_end_success`.

Keep `call_deploy_demo` unchanged (demo deploy stays as Call Activity).

- [ ] **Step 2: Update versionTag to 2.0**

---

### Task 8: Deploy all BPMN to Zeebe

- [ ] **Step 1: Deploy updated processes**

```bash
# Deploy all CI/CD BPMN files as a single deployment (atomic)
curl -s -X POST "http://localhost:8088/v2/deployments" \
  -H "Authorization: Bearer $TOKEN" \
  -F "resources=@bpmn/ci-cd/pr-review.bpmn" \
  -F "resources=@bpmn/ci-cd/deploy-process.bpmn" \
  -F "resources=@bpmn/ci-cd/feature-to-production.bpmn" \
  -F "resources=@bpmn/ci-cd/main-to-staging-sync.bpmn" \
  -F "resources=@bpmn/ci-cd/upstream-sync.bpmn"
```

- [ ] **Step 2: Verify in Operate**

Check http://10.1.1.74:8088/operate that all processes show new versions.

- [ ] **Step 3: Rebuild and restart worker**

```bash
docker compose -f docker-compose-full.yaml build python-worker
docker compose -f docker-compose-full.yaml up -d python-worker
```

- [ ] **Step 4: Configure GitHub webhook**

In GitHub → repo settings → Webhooks → Edit:
- Add "Pushes" event

- [ ] **Step 5: Smoke test — create a test PR to verify review starts**

Create a test PR to `main`. Verify in Operate:
1. A new `pr-review` process instance starts
2. If pipeline is running, it catches `msg_review_done`
3. Push to staging triggers `deploy-process`
