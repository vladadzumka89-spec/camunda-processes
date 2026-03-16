# Hardened Deploy Pipeline Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make deploy pipeline resilient — DB checkpoint before module-update on production, try/finally for service restart, smoke-test fails fast, save-state is fault-tolerant.

**Architecture:** Add `db-checkpoint` task type for production DB backup via pgBackRest before module-update. Wrap module-update in try/finally to guarantee DB restart. Make smoke-test raise on failure instead of returning false. Add `db_restore_command` config for production rollback.

**Tech Stack:** Python 3.12, pyzeebe, asyncssh, Camunda 8.8 Zeebe, BPMN XML

**Spec:** `docs/superpowers/specs/2026-03-16-hardened-deploy-pipeline-design.md`

---

## Chunk 1: Python handler changes

### Task 1: Add `db-checkpoint` handler

**Files:**
- Modify: `worker/handlers/deploy.py` (after rollback handler, ~line 471)
- Modify: `worker/handlers/__init__.py` (already registers deploy handlers, no change needed — db-checkpoint is registered inside `register_deploy_handlers`)
- Test: `tests/test_handlers_deploy.py`

- [ ] **Step 1: Write failing test**

Add to `tests/test_handlers_deploy.py` after the rollback tests:

```python
# ── db-checkpoint ──────────────────────────────────────────

@pytest.mark.asyncio
async def test_db_checkpoint_production(handlers, mock_ssh):
    """db-checkpoint runs backup command on production server."""
    mock_ssh.run.side_effect = [OK()]  # checkpoint command
    result = await handlers["db-checkpoint"](
        server_host="production",
        container="odoo19",
    )
    assert result["checkpoint_created"] is True
    cmd = mock_ssh.run.call_args_list[0].args[1]
    assert "pgbackrest" in cmd or "flock" in cmd


@pytest.mark.asyncio
async def test_db_checkpoint_custom_command(handlers, mock_ssh):
    """db-checkpoint uses custom command when provided."""
    mock_ssh.run.side_effect = [OK()]
    result = await handlers["db-checkpoint"](
        server_host="production",
        db_checkpoint_command="pg_dump -Fc mydb > /tmp/backup.custom",
    )
    assert result["checkpoint_created"] is True
    cmd = mock_ssh.run.call_args_list[0].args[1]
    assert "pg_dump" in cmd
```

Note: test helpers `OK()` = `_make_ssh_result()`, `handlers` fixture extracts all handlers. The `handlers` fixture uses `app_config` which has `production` server defined in `conftest.py` — check if production server exists in test config. If not, use `staging` for tests and verify the handler logic separately.

- [ ] **Step 2: Run tests to verify they fail**

Run: `docker run --rm -v /opt/camunda/docker-compose-8.8:/app -w /app docker-compose-88-python-worker sh -c "pip install pytest pytest-asyncio -q && python -m pytest tests/test_handlers_deploy.py::test_db_checkpoint_production tests/test_handlers_deploy.py::test_db_checkpoint_custom_command -v"`

- [ ] **Step 3: Implement handler**

Add to `worker/handlers/deploy.py` after the rollback handler (~line 471), before the helpers section:

```python
    # ── db-checkpoint ─────────────────────────────────────────

    @worker.task(task_type="db-checkpoint", timeout_ms=600_000)
    async def db_checkpoint(
        server_host: str,
        db_checkpoint_command: str = "",
        container: str = "",
        **kwargs: Any,
    ) -> dict:
        """Create DB checkpoint before module update (production only)."""
        server = config.resolve_server(server_host)
        ctr = container or server.container

        if db_checkpoint_command:
            cmd = db_checkpoint_command
        else:
            cmd = (
                f"docker exec {ctr}-db su - postgres -c "
                f"\"flock -n /tmp/pgbr.lock /usr/bin/pgbackrest "
                f"--stanza=main --type=full backup\""
            )

        await ssh.run(server, cmd, check=True, timeout=540)
        logger.info("db-checkpoint on %s: completed", server.host)
        return {"checkpoint_created": True}
```

- [ ] **Step 4: Run tests to verify they pass**

- [ ] **Step 5: Commit**

```bash
git add worker/handlers/deploy.py tests/test_handlers_deploy.py
git commit -m "feat: add db-checkpoint handler for production DB backup

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: try/finally in module-update

**Files:**
- Modify: `worker/handlers/deploy.py:291-307`
- Test: `tests/test_handlers_deploy.py`

- [ ] **Step 1: Write failing test**

```python
@pytest.mark.asyncio
async def test_module_update_restarts_db_on_failure(handlers, mock_ssh, mock_sleep):
    """If module-update fails, DB should be restarted in finally block."""
    mock_ssh.run.side_effect = [OK("password123")]  # _get_db_password
    mock_ssh.run_in_repo.side_effect = [
        OK(),                    # find __pycache__
        OK(),                    # docker compose stop
        RemoteCommandError("odoo-bin failed", 1, ""),  # module update fails
        OK(),                    # finally: docker compose start db
    ]
    with pytest.raises(RemoteCommandError):
        await handlers["module-update"](
            server_host="staging",
            changed_modules="sale_management",
        )
    # Verify docker compose start db was called in finally
    calls = mock_ssh.run_in_repo.call_args_list
    assert "docker compose start db" in calls[-1].args[1]
```

Note: `RemoteCommandError` imported from `worker.ssh`.

- [ ] **Step 2: Implement try/finally**

In `worker/handlers/deploy.py`, replace lines 291-307 with:

```python
        # Stop all services
        await ssh.run_in_repo(server, "docker compose stop", timeout=60)

        try:
            # Start DB + run module update
            await ssh.run_in_repo(
                server,
                f"docker compose start db && sleep 3 && "
                f"timeout 2000 docker compose run --rm --no-deps web "
                f"odoo-bin -d {db} -u {update_modules} "
                f"--db_password='{db_password}' "
                f"--stop-after-init --no-http --log-level=warn",
                check=True,
                timeout=2100,
            )

            # Success: restart full stack
            await ssh.run_in_repo(server, "docker compose up -d", check=True, timeout=120)
        finally:
            # Ensure at least DB is running (rollback needs it)
            try:
                await ssh.run_in_repo(server, "docker compose start db", check=False, timeout=60)
            except Exception:
                pass
```

- [ ] **Step 3: Run all module-update tests**

Run: `docker run --rm -v /opt/camunda/docker-compose-8.8:/app -w /app docker-compose-88-python-worker sh -c "pip install pytest pytest-asyncio -q && python -m pytest tests/test_handlers_deploy.py -k module_update -v"`

- [ ] **Step 4: Fix any broken existing tests**

The existing `test_module_update_*` tests mock `run_in_repo.side_effect` with a specific number of calls. The new try/finally adds one more call (`docker compose start db` in finally). Update side_effects to include this extra call.

- [ ] **Step 5: Commit**

```bash
git add worker/handlers/deploy.py tests/test_handlers_deploy.py
git commit -m "fix: try/finally in module-update — DB always restarts on failure

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: smoke-test raises on failure

**Files:**
- Modify: `worker/handlers/deploy.py:345-395`
- Test: `tests/test_handlers_deploy.py`

- [ ] **Step 1: Write failing test**

```python
@pytest.mark.asyncio
async def test_smoke_test_raises_on_failure(handlers, mock_ssh, mock_sleep):
    """Smoke test should raise RuntimeError when errors detected."""
    mock_ssh.run.side_effect = [OK("password123")]  # _get_db_password
    mock_ssh.run_in_repo.side_effect = [
        _make_ssh_result(stdout="ERROR: SyntaxError in module\n", exit_code=1),  # smoke test
    ]
    with pytest.raises(RuntimeError, match="Smoke test failed"):
        await handlers["smoke-test"](server_host="staging")
```

- [ ] **Step 2: Implement change**

In `worker/handlers/deploy.py` smoke_test handler, replace the end (after error_lines parsing):

```python
        smoke_passed = result.exit_code == 0 and not error_lines

        if not smoke_passed:
            error_summary = "; ".join(error_lines[:3]) if error_lines else f"exit code {result.exit_code}"
            raise RuntimeError(f"Smoke test failed on {server.host}: {error_summary}")

        logger.info("smoke-test on %s: passed=True", server.host)
        return {"smoke_passed": True}
```

- [ ] **Step 3: Update existing smoke tests that expect `smoke_passed: False`**

Tests like `test_smoke_test_fails_on_error`, `test_smoke_test_fails_on_exit_code`, `test_smoke_test_detects_critical` etc. currently assert `result["smoke_passed"] is False`. They now need to assert `pytest.raises(RuntimeError)`.

- [ ] **Step 4: Run all smoke tests**

- [ ] **Step 5: Commit**

```bash
git add worker/handlers/deploy.py tests/test_handlers_deploy.py
git commit -m "fix: smoke-test raises RuntimeError on failure — triggers BPMN rollback

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Rollback with DB restore for production

**Files:**
- Modify: `worker/handlers/deploy.py:440-471` (rollback handler)
- Modify: `worker/config.py` (add `db_restore_command`)
- Test: `tests/test_handlers_deploy.py`

- [ ] **Step 1: Add `db_restore_command` to AppConfig**

In `worker/config.py`, add to `AppConfig` class (after `openrouter_api_key`):

```python
    db_restore_command: str = ''
```

And in `from_env()` (after `openrouter_api_key` line):

```python
            db_restore_command=os.getenv('DB_RESTORE_COMMAND', ''),
```

- [ ] **Step 2: Write failing test**

```python
@pytest.mark.asyncio
async def test_rollback_production_restores_db(handlers, mock_ssh):
    """Rollback on production should restore DB before git checkout."""
    mock_ssh.run_in_repo.side_effect = [
        OK(),  # docker compose stop
        OK(),  # db restore command
        OK(),  # docker compose start db
        OK(),  # git checkout
        OK(),  # docker compose up -d --force-recreate
    ]
    result = await handlers["rollback"](
        server_host="production",
        old_commit="abc123",
        branch="main",
        db_restore_command="pgbackrest restore",
    )
    calls = [c.args[1] for c in mock_ssh.run_in_repo.call_args_list]
    assert "docker compose stop" in calls[0]
    assert "pgbackrest restore" in calls[1]
    assert "docker compose start db" in calls[2]
    assert "git checkout" in calls[3]


@pytest.mark.asyncio
async def test_rollback_staging_no_db_restore(handlers, mock_ssh):
    """Rollback on staging should NOT restore DB."""
    mock_ssh.run_in_repo.side_effect = [
        OK(),  # git checkout
        OK(),  # docker compose up -d
    ]
    await handlers["rollback"](
        server_host="staging",
        old_commit="abc123",
        branch="staging",
    )
    calls = [c.args[1] for c in mock_ssh.run_in_repo.call_args_list]
    assert not any("restore" in c for c in calls)
```

- [ ] **Step 3: Implement production rollback**

Replace rollback handler:

```python
    @worker.task(task_type="rollback", timeout_ms=300_000)
    async def rollback(
        server_host: str,
        old_commit: str = "none",
        branch: str = "",
        db_restore_command: str = "",
        repo_dir: str = "",
        **kwargs: Any,
    ) -> dict:
        """Rollback to previous commit. On production, restores DB first."""
        server = config.resolve_server(server_host)

        if old_commit in ("none", ""):
            logger.warning("rollback on %s: no previous commit, skipping", server.host)
            return {}

        # Production: restore DB from checkpoint
        is_prod = config.resolve_server(server_host) == config.servers.get("production")
        restore_cmd = db_restore_command or config.db_restore_command

        if is_prod and restore_cmd:
            logger.info("rollback on %s: restoring DB from checkpoint", server.host)
            await ssh.run_in_repo(server, "docker compose stop", timeout=60)
            await ssh.run(server, restore_cmd, check=True, timeout=600)
            await ssh.run_in_repo(server, "docker compose start db", check=True, timeout=60)

        # Git checkout
        if branch:
            await ssh.run_in_repo(
                server,
                f"git checkout -B {branch} {old_commit}",
                check=True,
            )
        else:
            await ssh.run_in_repo(server, f"git checkout {old_commit}", check=True)

        await ssh.run_in_repo(
            server,
            "docker compose up -d --force-recreate",
            check=True,
            timeout=120,
        )
        logger.info("rollback on %s to %s (db_restored=%s)", server.host, old_commit[:8], bool(is_prod and restore_cmd))
        return {}
```

- [ ] **Step 4: Run all rollback tests**

- [ ] **Step 5: Commit**

```bash
git add worker/handlers/deploy.py worker/config.py tests/test_handlers_deploy.py
git commit -m "feat: production rollback restores DB from checkpoint

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: save-deploy-state error handling

**Files:**
- Modify: `worker/handlers/deploy.py:415-436`
- Test: `tests/test_handlers_deploy.py`

- [ ] **Step 1: Write failing test**

```python
@pytest.mark.asyncio
async def test_save_deploy_state_handles_ssh_error(handlers, mock_ssh):
    """save-deploy-state should not raise on SSH failure — returns state_saved=False."""
    mock_ssh.run.side_effect = RemoteCommandError("SSH failed", 1, "")
    result = await handlers["save-deploy-state"](
        server_host="staging",
        branch="staging",
        new_commit="abc123",
    )
    assert result["state_saved"] is False
```

- [ ] **Step 2: Implement try/except**

```python
    @worker.task(task_type="save-deploy-state", timeout_ms=30_000)
    async def save_deploy_state(
        server_host: str,
        branch: str,
        new_commit: str,
        repo_dir: str = "",
        **kwargs: Any,
    ) -> dict:
        """Save deployed commit hash to state file. Best-effort — does not fail deploy."""
        server = config.resolve_server(server_host)
        repo = repo_dir or server.repo_dir

        try:
            safe_branch = branch.replace("/", "_")
            await ssh.run(
                server,
                f"mkdir -p {repo}/.deploy-state && chmod 700 {repo}/.deploy-state && "
                f"echo '{new_commit}' > {repo}/.deploy-state/deploy_state_{safe_branch} && "
                f"chmod 600 {repo}/.deploy-state/deploy_state_{safe_branch}",
                check=True,
            )
            logger.info("save-deploy-state on %s: %s → %s", server.host, branch, new_commit[:8])
            return {"state_saved": True}
        except Exception as exc:
            logger.warning("Failed to save deploy state on %s: %s", server.host, exc)
            return {"state_saved": False}
```

- [ ] **Step 3: Run tests**

- [ ] **Step 4: Commit**

```bash
git add worker/handlers/deploy.py tests/test_handlers_deploy.py
git commit -m "fix: save-deploy-state is fault-tolerant — won't rollback successful deploy

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

## Chunk 2: BPMN changes + deploy

### Task 6: Update deploy-process.bpmn — production checkpoint + retries

**Files:**
- Modify: `bpmn/ci-cd/deploy-process.bpmn`

- [ ] **Step 1: Add `gw_is_production` + `ST_db_checkpoint` before module-update**

Insert between `gw_merge_docker` and `task_module_update`:

```xml
    <!-- ── PRODUCTION CHECKPOINT ───────────────────────────── -->
    <bpmn:exclusiveGateway id="gw_is_production" name="Production?" default="f_not_production">
      <bpmn:incoming>f09</bpmn:incoming>
      <bpmn:outgoing>f_is_production</bpmn:outgoing>
      <bpmn:outgoing>f_not_production</bpmn:outgoing>
    </bpmn:exclusiveGateway>

    <bpmn:serviceTask id="ST_db_checkpoint" name="DB Checkpoint (pgBackRest)">
      <bpmn:documentation>Creates full DB backup before module update. Production only.</bpmn:documentation>
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="db-checkpoint" retries="2" />
      </bpmn:extensionElements>
      <bpmn:incoming>f_is_production</bpmn:incoming>
      <bpmn:outgoing>f_checkpoint_done</bpmn:outgoing>
    </bpmn:serviceTask>

    <bpmn:exclusiveGateway id="gw_merge_checkpoint">
      <bpmn:incoming>f_not_production</bpmn:incoming>
      <bpmn:incoming>f_checkpoint_done</bpmn:incoming>
      <bpmn:outgoing>f_to_module_update</bpmn:outgoing>
    </bpmn:exclusiveGateway>
```

Update flows:
- `f09`: change targetRef from `task_module_update` to `gw_is_production`
- Add: `f_is_production` with condition `=server_host = "production"`
- Add: `f_not_production` (default)
- Add: `f_checkpoint_done`
- Add: `f_to_module_update` targeting `task_module_update`
- Update `task_module_update` incoming from `f09` to `f_to_module_update`

- [ ] **Step 2: Update retries on idempotent tasks**

Change in BPMN XML:
- `detect-modules`: `retries="1"` → `retries="3"`
- `cache-clear`: `retries="1"` → `retries="3"`
- `save-deploy-state`: `retries="1"` → `retries="3"`
- `docker-up`: `retries="1"` → `retries="3"`

Keep `smoke-test` at `retries="1"` and `module-update` at `retries="1"`.

- [ ] **Step 3: Update versionTag to 3.0**

- [ ] **Step 4: Add DI shapes/edges for new elements**

- [ ] **Step 5: Run layout checker**

```bash
python3 bpmn_layout_checker.py bpmn/ci-cd/deploy-process.bpmn
```

- [ ] **Step 6: Deploy to Zeebe**

```bash
TOKEN=$(curl -s -X POST "http://localhost:18080/auth/realms/camunda-platform/protocol/openid-connect/token" \
  -d "grant_type=client_credentials&client_id=orchestration&client_secret=oUV-An_2FED-qYTT" \
  | python3 -c "import sys,json;print(json.load(sys.stdin)['access_token'])")

curl -s -X POST "http://localhost:8088/v2/deployments" \
  -H "Authorization: Bearer $TOKEN" \
  -F "resources=@bpmn/ci-cd/deploy-process.bpmn"
```

- [ ] **Step 7: Rebuild and restart worker**

```bash
docker compose -f docker-compose-full.yaml build python-worker
docker rm -f python-worker; docker compose -f docker-compose-full.yaml up -d python-worker
```

- [ ] **Step 8: Commit**

```bash
git add bpmn/ci-cd/deploy-process.bpmn
git commit -m "feat: hardened deploy — DB checkpoint, increased retries, version 3.0

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"
```

---

### Task 7: Run full test suite

- [ ] **Step 1: Run all tests**

```bash
docker run --rm -v /opt/camunda/docker-compose-8.8:/app -w /app docker-compose-88-python-worker \
  sh -c "pip install pytest pytest-asyncio -q && python -m pytest tests/ -v --ignore=tests/integration --ignore=tests/test_fop_limit_monitor.py"
```

Expected: All pass.

- [ ] **Step 2: Manual smoke test — trigger deploy on staging**

```bash
# Trigger deploy without clickbot
TOKEN=... && curl -s -X POST "http://localhost:8088/v2/messages/publication" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"name":"msg_deploy_trigger","correlationKey":"test-hardened","variables":{"trigger_sha":"test-hardened","server_host":"10.1.1.65","ssh_user":"root","repo_dir":"/opt/odoo-enterprise","db_name":"odoo19","container":"odoo19","branch":"staging","run_smoke_test":true,"skip_clickbot":true,"odoo_project_id":237},"timeToLive":3600000}'
```

Verify in logs: no checkpoint (not production), module-update with try/finally, smoke-test passes.
