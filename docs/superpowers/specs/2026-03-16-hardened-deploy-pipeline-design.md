# Hardened Deploy Pipeline

**Date:** 2026-03-16
**Status:** Approved

## Problem

Deploy pipeline has several reliability gaps:
1. No DB backup before module-update — if migration breaks schema, rollback only restores code, not DB
2. module-update stops all services (`docker compose stop`) without try/finally — if update fails, services stay down
3. smoke_passed result is ignored by BPMN — process continues to happy path even when smoke test fails
4. Idempotent tasks have retries=1 — one network hiccup = full rollback
5. save-deploy-state failure triggers rollback of a successful deploy

## Design

### 1. DB Checkpoint Before Module-Update (Production Only)

New Service Task `ST_db_checkpoint` inserted before `task_module_update` in deploy-process.bpmn.

**XOR gateway `gw_is_production`** checks whether current deploy is production:
- Production → `ST_db_checkpoint` → merge gateway → module-update
- Not production → skip → merge gateway → module-update

**Production detection** — uses `config.resolve_server(server_host)` in Python. A server is production if its resolved name is `"production"`. In BPMN FEEL: `=server_host = "production"` (the webhook/pipeline always passes the server name, not IP).

`ST_db_checkpoint` executes via new task type `db-checkpoint` in deploy.py handler.

**Handler signature:**
```python
@worker.task(task_type="db-checkpoint", timeout_ms=600_000)
async def db_checkpoint(
    server_host: str,
    db_checkpoint_command: str = "",
    container: str = "",
    **kwargs: Any,
) -> dict:
```

**Default checkpoint command** constructed from config:
```python
ctr = container or server.container
default_cmd = (
    f"docker exec {ctr}-db su - postgres -c "
    f"\"flock -n /tmp/pgbr.lock /usr/bin/pgbackrest --stanza=main --type=full backup\""
)
cmd = db_checkpoint_command or default_cmd
```

**Configuration source:** `db_checkpoint_command` can be passed as:
- Process variable (from webhook or Call Activity input mapping)
- If not set, constructed from `ServerConfig.container` (e.g., `odoo19-db`)

**Retries:** 2 (backup is not idempotent if it completes partially, but flock prevents concurrent runs, so a retry after timeout/network error is safe).

### 2. try/finally for module-update

In `worker/handlers/deploy.py`, wrap the module-update sequence so that the DB service is always restarted on failure. The finally block starts only the DB (not full stack), because the BPMN rollback will handle full restart with correct code version:

```python
await ssh.run_in_repo(server, "docker compose stop", timeout=60)
try:
    await ssh.run_in_repo(server, "docker compose start db", check=True, timeout=60)
    await _sleep(3)
    await ssh.run_in_repo(server, f"timeout 2000 docker compose run ...", check=True, timeout=2100)
    # Success: restart full stack
    await ssh.run_in_repo(server, "docker compose up -d", check=True, timeout=120)
finally:
    # On failure: ensure at least DB is running (rollback needs it)
    # On success: this is a no-op (DB already running as part of compose up)
    try:
        await ssh.run_in_repo(server, "docker compose start db", check=False, timeout=60)
    except Exception:
        pass  # Best effort — rollback will handle it
```

This avoids the "broken state live" window: on failure, only DB starts (not the web app with broken modules), and BPMN rollback handles `git checkout + docker compose up -d --force-recreate`.

### 3. smoke-test Raises on Failure

Make smoke-test handler **raise RuntimeError** when smoke fails, instead of returning `smoke_passed: false`:

```python
if not smoke_passed:
    raise RuntimeError(f"Smoke test failed: {'; '.join(error_lines[:3])}")
return {"smoke_passed": True}
```

No BPMN change needed — existing Error Event Subprocess catches BPMN Error (thrown by exception handler when retries exhausted).

**Important:** smoke-test retries MUST remain at 1. With retries=1, the first failure immediately triggers BPMN Error → rollback. If retries were increased, smoke failure would be retried (pointless — same code will fail again). This is documented here explicitly to prevent accidental change.

### 4. DB Restore in Rollback (Production Only)

In `worker/handlers/deploy.py` rollback handler, add DB restore step for production:

```python
async def rollback(
    server_host: str,
    old_commit: str = "",
    branch: str = "",
    db_restore_command: str = "",
    **kwargs: Any,
) -> dict:
```

**Production detection:** same as section 1 — `config.resolve_server(server_host)` name check.

**Rollback sequence for production:**
1. Stop services: `docker compose stop`
2. Restore DB from checkpoint: execute `db_restore_command` (or default pgBackRest restore)
3. Start DB: `docker compose start db`
4. `git checkout -B {branch} {old_commit}`
5. `docker compose up -d --force-recreate`

**Rollback sequence for staging (unchanged):**
1. `git checkout -B {branch} {old_commit}`
2. `docker compose up -d --force-recreate`

**Configuration source:** `db_restore_command` from:
- Process variable (input mapping from pipeline or webhook)
- Environment variable `DB_RESTORE_COMMAND` in `.env.camunda`
- If neither set, skip DB restore with warning log

### 5. Increase Retries on Idempotent Tasks

| Task | Current | New | Reason |
|------|---------|-----|--------|
| detect-modules | 1 | 3 | Read-only, safe to retry |
| cache-clear | 1 | 3 | Idempotent SQL DELETE |
| save-deploy-state | 1 | 3 | Idempotent file write |
| docker-up | 1 | 3 | Idempotent compose up |
| smoke-test | 1 | **1 (keep)** | Must fail immediately to trigger rollback |
| module-update | 1 | **1 (keep)** | Not idempotent — partial migration cannot be retried |

### 6. save-deploy-state Error Handling

Handler catches `Exception` (not `BaseException`) — logs warning, returns success:

```python
try:
    # ... write state file
    return {"state_saved": True}
except Exception as exc:
    logger.warning("Failed to save deploy state on %s: %s", server.host, exc)
    return {"state_saved": False}
```

This prevents rollback of a successful deploy due to a non-critical file write failure.

### 7. BPMN Version Tag

Update `versionTag` from `"2.0"` to `"3.0"` in deploy-process.bpmn.

## Files to Modify

| File | Change |
|------|--------|
| `worker/handlers/deploy.py` | Add `db-checkpoint` handler (11th task type), try/finally in module-update, smoke-test raises on failure, rollback with DB restore for production, save-state error handling |
| `worker/config.py` | Add `db_restore_command` to config (from env var `DB_RESTORE_COMMAND`) |
| `bpmn/ci-cd/deploy-process.bpmn` | Add `gw_is_production` + `ST_db_checkpoint` + merge gateway before module-update; update retries on idempotent tasks; bump versionTag to 3.0 |
| `tests/test_handlers_deploy.py` | Tests for: db-checkpoint handler, module-update try/finally (services restart on failure), smoke-test raises RuntimeError, rollback with DB restore, save-state exception handling |

## Out of Scope

- Specific pgBackRest restore command (sysadmin will provide, placeholder in config)
- Staging DB backup
- Changes to clickbot or other handlers
- docker-compose.clickbot.yml (not in repo)
