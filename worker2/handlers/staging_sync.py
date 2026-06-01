"""Nightly staging sync handlers — dump prod DB, anonymize, transfer to staging.

Pipeline:
  1. staging-dump:      prod: pg_dump odoo19 | zstd → /tmp/dump.zst на козак_демо
  2. staging-anonymize: kozak_demo: sync.sh deploy → анонімізована БД в kozak-staging контейнері
  3. staging-export:    kozak pg_dump → SFTP → staging: замінює odoo19 DB напряму в odoo19-db
"""

from __future__ import annotations

import asyncio
import logging
import re
import shlex
from datetime import datetime
from pathlib import Path
from typing import Any

import asyncssh
from pyzeebe import ZeebeWorker

from ..auth import ZeebeAuthConfig, zeebe_client
from ..config import AppConfig
from ..errors import (
    BpmnError,
    StagingAnonymizeError,
    StagingDumpError,
    StagingExportError,
)
from ..ssh import AsyncSSHClient
from .. import staging_lock

logger = logging.getLogger(__name__)

DUMP_PATH      = "/tmp/nightly_staging_dump.sql.zst"
TRANSFER_PATH  = "/tmp/anon_transfer.sql.zst"
ANON_SNAPSHOT  = "/opt/odoo-enterprise/backups/anon_latest.sql.zst"
SYNC_SCRIPT    = "/opt/odoo-enterprise/scripts/db_anonymize/sync.sh"
SYNC_LOG_PATH  = "/opt/odoo-enterprise/scripts/db_anonymize/sync.log"
INCIDENTS_FILE = Path("/app/staging-sync-incidents.md")
KOZAK_SYNC_REPO = "/opt/odoo-enterprise"

NFS_HOST      = "10.1.1.99"
NFS_DEST_DIR  = "/mnt/borys-nfs-import-db"
NFS_TMP_PATH  = "/tmp/nfs_deliver_tmp.sql.zst"

# kozak_demo: тимчасові staging контейнери де sync.sh будує анонімізовану БД
KOZAK_STAGING_DB     = "odoo_staging"
KOZAK_STAGING_DB_CTR = "odoo19-staging-db"
KOZAK_PG_USER        = "odoo"

# Staging сервер: замінюємо odoo19 DB напряму в основному контейнері
STG_DB      = "odoo19"
STG_DB_CTR  = "odoo19-db"
STG_CTR     = "odoo19"
STG_NETWORK = "odoo-enterprise_odoo-network"
STG_VOLUME  = "odoo-enterprise_odoo-web-data"
STG_PG_USER = "odoo"
STG_IMAGE   = "odoo-custom:19.0"

_SECRET_PATTERNS = (
    re.compile(r"https://x-access-token:[^@\s]+@github\.com/"),
    re.compile(r"\bgh[pousr]_[A-Za-z0-9_]{20,}\b"),
    re.compile(r"\bgithub_pat_[A-Za-z0-9_]{20,}\b"),
)


_SFTP_CHUNK_TIMEOUT = 120   # seconds per 2MB chunk read/write
_SFTP_CONNECT_TIMEOUT = 30  # seconds for SSH handshake

_STOPPED_CONTAINER_STATES = {"missing", "exited", "dead", "created", "removing"}
_STAGING_STOP_GRACE_SECONDS = 90
_STAGING_STOP_COMMAND_TIMEOUT = 180


def _staging_bpmn_error(exc: Exception, error_cls: type[BpmnError]) -> BpmnError:
    """Wrap arbitrary failures in the BPMN error code the diagram catches."""
    if isinstance(exc, error_cls):
        return exc
    return error_cls(str(exc), variables={"error_type": "infra"})


def _git_auth_url(pat: str, repo: str) -> str:
    """Build an authenticated GitHub URL for non-interactive server-side fetches."""
    return f"https://x-access-token:{pat}@github.com/{repo}.git"


def _redact_secrets(text: str, *secrets: str) -> str:
    """Remove tokens from command output before logging, incidents, or BPMN errors."""
    redacted = text
    for secret in secrets:
        if secret:
            redacted = redacted.replace(secret, "***")
    for pattern in _SECRET_PATTERNS:
        redacted = pattern.sub("https://x-access-token:***@github.com/" if pattern is _SECRET_PATTERNS[0] else "***", redacted)
    return redacted


def _kozak_sync_preflight_command(repo_dir: str) -> str:
    """Build a fail-fast preflight for kozak_demo before the long restore starts."""
    command = r"""
set -euo pipefail
cd __REPO_DIR__

script="scripts/db_anonymize/sync.sh"
conf="scripts/db_anonymize/sync.conf"

[ -x "$script" ] || { echo "sync.sh is missing or not executable: $script" >&2; exit 1; }
[ -f "$conf" ] || { echo "sync.conf is missing: copy scripts/db_anonymize/sync.conf.example to sync.conf" >&2; exit 1; }
[ -f "docker-compose.staging.yml" ] || { echo "docker-compose.staging.yml is missing" >&2; exit 1; }

python3 - <<'PY'
import importlib.util
import sys

missing = [mod for mod in ("psycopg2", "yaml") if importlib.util.find_spec(mod) is None]
if missing:
    print(
        "Missing Python dependency for db anonymize: "
        + ", ".join(missing)
        + ". Install python3-psycopg2/python3-yaml or scripts/db_anonymize/requirements.txt.",
        file=sys.stderr,
    )
    raise SystemExit(1)
PY

set -a
# shellcheck source=/dev/null
source "$conf"
set +a

POSTGRES_IMAGE="${POSTGRES_IMAGE:-odoo-db:16}"
STAGING_DB_PORT="${STAGING_DB_PORT:-5433}"
STAGING_ODOO_PORT="${STAGING_ODOO_PORT:-8070}"
STAGING_DB_CONTAINER="${STAGING_DB_CONTAINER:-odoo19-staging-db}"
STAGING_ODOO_CONTAINER="${STAGING_ODOO_CONTAINER:-odoo19-staging}"

docker image inspect "$POSTGRES_IMAGE" >/dev/null 2>&1 \
  || { echo "Docker image not present locally: $POSTGRES_IMAGE" >&2; exit 1; }

conflicts="$(
  docker ps --format '{{.Names}} {{.Ports}}' | while read -r name ports; do
    case "$name" in
      "$STAGING_DB_CONTAINER"|"$STAGING_ODOO_CONTAINER") continue ;;
    esac
    case "$ports" in
      *":${STAGING_DB_PORT}->"*|*":${STAGING_ODOO_PORT}->"*) echo "$name $ports" ;;
    esac
  done
)"

if [ -n "$conflicts" ]; then
  echo "Configured staging ports are already used by other containers:" >&2
  echo "$conflicts" >&2
  exit 1
fi
""".strip()
    return command.replace("__REPO_DIR__", shlex.quote(repo_dir))


async def _run_git_checked(
    ssh: AsyncSSHClient,
    server: Any,
    command: str,
    *,
    timeout: int,
    pat: str,
    error_cls: type[BpmnError],
    label: str,
) -> None:
    """Run a git command that may contain a PAT without letting ssh.py log it raw."""
    result = await ssh.run(server, command, timeout=timeout, check=False)
    if result.success:
        return

    output = "\n".join(part for part in (result.stderr.strip(), result.stdout.strip()) if part)
    safe_output = _redact_secrets(output or "(no output)", pat)
    raise error_cls(
        f"{label} failed on {server.host} (exit code {result.exit_code}): {safe_output}",
        variables={"error_type": "infra"},
    )


async def _reset_staging_code_to_deployed_commit(
    ssh: AsyncSSHClient,
    staging: Any,
    config: AppConfig,
    deployed_commit: str,
) -> str:
    """Reset local and remote staging code only after the replacement DB is ready."""
    deploy_pat = config.github.deploy_pat
    if not deploy_pat:
        raise StagingExportError(
            "DEPLOY_PAT is empty; cannot reset staging branch non-interactively",
            variables={"error_type": "infra"},
        )

    target = (deployed_commit or "main").strip()
    if target != "main" and not re.fullmatch(r"[0-9a-fA-F]{7,40}", target):
        raise StagingExportError(
            f"Invalid deployed_commit for staging reset: {target!r}",
            variables={"error_type": "infra"},
        )

    repo_dir = shlex.quote(staging.repo_dir)
    push_url = shlex.quote(_git_auth_url(deploy_pat, config.github.repository))
    target_ref = "refs/remotes/ci/main" if target == "main" else shlex.quote(target)

    command = (
        "set -e; "
        f"cd {repo_dir}; "
        f"git config --global --add safe.directory {repo_dir} 2>/dev/null || true; "
        f"git fetch {push_url} +refs/heads/main:refs/remotes/ci/main; "
        f"git cat-file -e {target_ref}^{{commit}}; "
        f"git reset --hard {target_ref}; "
        "git clean -fd; "
        f"git push --force {push_url} HEAD:staging"
    )
    await _run_git_checked(
        ssh,
        staging,
        command,
        timeout=180,
        pat=deploy_pat,
        error_cls=StagingExportError,
        label=f"staging code reset to {target[:12]}",
    )
    logger.info("staging-export: reset staging code to %s", target[:12])
    return target


async def _container_status(
    ssh: AsyncSSHClient,
    server: Any,
    container: str,
) -> str:
    """Return Docker container status, or ``missing`` when it does not exist."""
    quoted = shlex.quote(container)
    result = await ssh.run(
        server,
        f"docker inspect -f '{{{{.State.Status}}}}' {quoted} 2>/dev/null || echo missing",
        timeout=20,
        check=False,
    )
    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    return lines[-1] if lines else "unknown"


async def _stop_container_for_db_replace(
    ssh: AsyncSSHClient,
    server: Any,
    container: str,
) -> None:
    """Stop staging web before DB replacement without false-failing on slow shutdown.

    Docker/Odoo can take slightly longer than the old 30s timeout to exit cleanly.
    If our SSH command times out but Docker has already stopped the container, this
    step is considered successful and the DB replacement may safely continue.
    """
    before = await _container_status(ssh, server, container)
    if before in _STOPPED_CONTAINER_STATES:
        logger.info("staging-export: %s already stopped (status=%s)", container, before)
        return

    quoted = shlex.quote(container)
    stop_script = f"""
status="$(docker inspect -f '{{{{.State.Status}}}}' {quoted} 2>/dev/null || echo missing)"
case "$status" in
  missing|exited|dead|created|removing) exit 0 ;;
esac

docker stop -t {_STAGING_STOP_GRACE_SECONDS} {quoted} 2>/dev/null || true

for _i in $(seq 1 15); do
  status="$(docker inspect -f '{{{{.State.Status}}}}' {quoted} 2>/dev/null || echo missing)"
  case "$status" in
    missing|exited|dead|created|removing) exit 0 ;;
  esac
  sleep 2
done

docker kill {quoted} 2>/dev/null || true

for _i in $(seq 1 15); do
  status="$(docker inspect -f '{{{{.State.Status}}}}' {quoted} 2>/dev/null || echo missing)"
  case "$status" in
    missing|exited|dead|created|removing) exit 0 ;;
  esac
  sleep 2
done

echo "{container} still $status after docker stop/kill" >&2
exit 1
""".strip()
    command = f"timeout 150 sh -c {shlex.quote(stop_script)}"

    try:
        await ssh.run(
            server,
            command,
            timeout=_STAGING_STOP_COMMAND_TIMEOUT,
            check=True,
        )
    except Exception as exc:
        status = await _container_status(ssh, server, container)
        if status in _STOPPED_CONTAINER_STATES:
            logger.warning(
                "staging-export: stop command for %s failed/timed out, "
                "but container is already stopped (status=%s): %s",
                container,
                status,
                exc,
            )
            return
        raise StagingExportError(
            f"Could not stop {container} on {server.host}; current status={status}: {exc}",
            variables={"error_type": "infra"},
        ) from exc

    after = await _container_status(ssh, server, container)
    if after not in _STOPPED_CONTAINER_STATES:
        raise StagingExportError(
            f"Could not stop {container} on {server.host}; current status={after}",
            variables={"error_type": "infra"},
        )

    logger.info("staging-export: %s stopped (status=%s)", container, after)


async def _start_container_best_effort(
    ssh: AsyncSSHClient,
    server: Any,
    container: str,
) -> None:
    """Best-effort recovery when export fails before DB mutation starts."""
    try:
        status = await _container_status(ssh, server, container)
        if status == "running":
            return
        await ssh.run(
            server,
            f"docker start {shlex.quote(container)}",
            timeout=60,
            check=False,
        )
        logger.info("staging-export: best-effort started %s after pre-restore failure", container)
    except Exception as exc:
        logger.warning(
            "staging-export: could not best-effort start %s after pre-restore failure: %s",
            container,
            exc,
        )


async def _stream_file(
    src_host: str, src_path: str,
    dst_host: str, dst_path: str,
    key_path: str,
) -> None:
    """Стримінг файлу між двома серверами через воркер (2MB chunks).

    keepalive_interval+keepalive_count_max: виявляє dead TCP з'єднання (~5 хв).
    wait_for на кожен chunk: гарантує що read/write не зависнуть вічно.
    """
    _ssh_kw = dict(
        username="root",
        client_keys=[key_path],
        known_hosts=None,
        connect_timeout=_SFTP_CONNECT_TIMEOUT,
        keepalive_interval=60,
        keepalive_count_max=5,
    )
    src = await asyncssh.connect(host=src_host, **_ssh_kw)
    dst = await asyncssh.connect(host=dst_host, **_ssh_kw)
    try:
        async with src.start_sftp_client() as src_sftp, \
                   dst.start_sftp_client() as dst_sftp:
            async with await src_sftp.open(src_path, "rb", encoding=None) as sf, \
                       await dst_sftp.open(dst_path, "wb", encoding=None) as df:
                transferred = 0
                while True:
                    chunk = await asyncio.wait_for(
                        sf.read(2 * 1024 * 1024), timeout=_SFTP_CHUNK_TIMEOUT,
                    )
                    if not chunk:
                        break
                    await asyncio.wait_for(df.write(chunk), timeout=_SFTP_CHUNK_TIMEOUT)
                    transferred += len(chunk)
                logger.info("stream_file: transferred %.1f MB", transferred / 1024 / 1024)
    finally:
        src.close()
        dst.close()


async def _capture_incident(
    step: str,
    error: str,
    kozak_host: str,
    key_path: str,
) -> None:
    """Fetch sync.log tail from kozak_demo and append incident to staging-sync-incidents.md."""
    sync_log_tail = ""
    try:
        async with asyncssh.connect(
            host=kozak_host, username="root", client_keys=[key_path],
            known_hosts=None, connect_timeout=15,
        ) as conn:
            r = await conn.run(f"tail -100 {SYNC_LOG_PATH} 2>/dev/null || echo '(sync.log not found)'")
            sync_log_tail = r.stdout.strip()
    except Exception as e:
        sync_log_tail = f"(could not fetch sync.log: {e})"

    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    entry = (
        f"\n## {ts} — {step} FAILED\n\n"
        f"**Error:** `{error}`\n\n"
        f"<details><summary>sync.log (last 100 lines)</summary>\n\n"
        f"```\n{sync_log_tail}\n```\n\n</details>\n"
    )
    try:
        with INCIDENTS_FILE.open("a", encoding="utf-8") as f:
            f.write(entry)
        logger.info("incident recorded → %s", INCIDENTS_FILE)
    except Exception as e:
        logger.error("could not write incident file: %s", e)
    logger.error("INCIDENT [%s]: %s", step, error)


async def _stream_dump_to_kozak(
    prod_host: str, kozak_host: str, dst_path: str, key_path: str,
    ssh: AsyncSSHClient,
    prod_server: Any,
) -> None:
    """Dump prod DB to temp file on prod, SFTP-stream to kozak_demo, cleanup prod."""
    prod_tmp = "/tmp/nightly_staging_dump_tmp.sql.zst"

    await ssh.run(
        prod_server,
        f"set -o pipefail; docker exec odoo19-db pg_dump -U odoo odoo19 | zstd -T0 > {prod_tmp}",
        timeout=3600,
        check=True,
    )
    await _stream_file(
        src_host=prod_host, src_path=prod_tmp,
        dst_host=kozak_host, dst_path=dst_path,
        key_path=key_path,
    )
    await ssh.run(prod_server, f"rm -f {prod_tmp}", check=False)


def register_staging_sync_handlers(
    worker: ZeebeWorker,
    config: AppConfig,
    ssh: AsyncSSHClient,
) -> None:
    """Register nightly staging sync task handlers."""

    kozak   = config.get_server("kozak_demo")
    staging = config.get_server("staging")
    prod    = config.get_server("production")

    # ── staging-dump ──────────────────────────────────────────

    @worker.task(task_type="staging-dump", timeout_ms=3_600_000, max_jobs_to_activate=1)
    async def staging_dump(**kwargs: Any) -> dict:
        """Dump production DB → kozak_demo via worker relay."""
        staging_lock.acquire()
        logger.info("staging-dump: lock acquired — deploys to staging blocked")
        try:
            logger.info("staging-dump: streaming pg_dump from %s → kozak_demo", prod.host)

            await _stream_dump_to_kozak(
                prod_host=prod.host,
                kozak_host=kozak.host,
                dst_path=DUMP_PATH,
                key_path=config.ssh_key_path,
                ssh=ssh,
                prod_server=prod,
            )

            check = await ssh.run(kozak, f"test -s {DUMP_PATH} && du -h {DUMP_PATH}", check=True)
            size = check.stdout.strip().split()[0] if check.stdout.strip() else "?"
            logger.info("staging-dump: dump ready — %s", size)

            # Capture the exact commit deployed on prod so staging uses the same code
            commit_r = await ssh.run(prod, "git -C /opt/odoo-enterprise rev-parse HEAD 2>/dev/null || echo main", check=False)
            deployed_commit = commit_r.stdout.strip() or "main"
            logger.info("staging-dump: prod deployed commit — %s", deployed_commit)
            return {"dump_path": DUMP_PATH, "deployed_commit": deployed_commit}
        except Exception as exc:
            await _capture_incident("staging-dump", str(exc), kozak.host, config.ssh_key_path)
            staging_lock.release()
            logger.info("staging-dump: lock released after failure")
            raise _staging_bpmn_error(exc, StagingDumpError) from exc

    # ── staging-anonymize ─────────────────────────────────────

    @worker.task(task_type="staging-anonymize", timeout_ms=7_200_000, max_jobs_to_activate=1)
    async def staging_anonymize(
        dump_path: str = DUMP_PATH,
        deployed_commit: str = "main",
        **kwargs: Any,
    ) -> dict:
        """Run sync.sh deploy on kozak_demo: restore → anonymize → local staging."""
        try:
            if not config.github.deploy_pat:
                raise StagingAnonymizeError(
                    "DEPLOY_PAT is empty; cannot sync kozak_demo sources non-interactively",
                    variables={"error_type": "infra"},
                )

            source_commit = (deployed_commit or "main").strip()
            if source_commit != "main" and not re.fullmatch(r"[0-9a-fA-F]{7,40}", source_commit):
                raise StagingAnonymizeError(
                    f"Invalid deployed_commit for kozak source sync: {source_commit!r}",
                    variables={"error_type": "infra"},
                )

            logger.info(
                "staging-anonymize: syncing scripts from main and src from %s on kozak_demo",
                source_commit[:12],
            )
            fetch_url = shlex.quote(_git_auth_url(config.github.deploy_pat, config.github.repository))
            kozak_repo = shlex.quote(KOZAK_SYNC_REPO)
            source_ref = "refs/remotes/ci/main" if source_commit == "main" else shlex.quote(source_commit)
            sync_sources_cmd = (
                "set -e; "
                f"cd {kozak_repo}; "
                f"git config --global --add safe.directory {kozak_repo} 2>/dev/null || true; "
                f"git fetch {fetch_url} +refs/heads/main:refs/remotes/ci/main; "
                f"git cat-file -e {source_ref}^{{commit}}; "
                "git checkout refs/remotes/ci/main -- scripts/ docker-compose.staging.yml; "
                f"git checkout {source_ref} -- src/"
            )
            await _run_git_checked(
                ssh,
                kozak,
                sync_sources_cmd,
                timeout=120,
                pat=config.github.deploy_pat,
                error_cls=StagingAnonymizeError,
                label="staging-anonymize source sync",
            )

            logger.info("staging-anonymize: running kozak_demo sync preflight")
            await ssh.run(
                kozak,
                _kozak_sync_preflight_command(KOZAK_SYNC_REPO),
                timeout=60,
                check=True,
            )

            logger.info("staging-anonymize: running sync.sh deploy on kozak_demo")
            await ssh.run(
                kozak,
                f"cd /opt/odoo-enterprise && {SYNC_SCRIPT} deploy {dump_path}",
                timeout=7200,
                check=True,
            )
            await ssh.run(kozak, f"rm -f {dump_path}", check=False)

            logger.info("staging-anonymize: done")
            return {}
        except Exception as exc:
            safe_error = _redact_secrets(
                str(exc),
                config.github.deploy_pat,
                config.github.enterprise_pat,
                config.github.token,
            )
            await _capture_incident("staging-anonymize", safe_error, kozak.host, config.ssh_key_path)
            staging_lock.release()
            logger.info("staging-anonymize: lock released after failure")
            raise _staging_bpmn_error(
                StagingAnonymizeError(safe_error, variables={"error_type": "infra"}),
                StagingAnonymizeError,
            ) from exc

    # ── staging-export ────────────────────────────────────────

    @worker.task(task_type="staging-export", timeout_ms=7_200_000, max_jobs_to_activate=1)
    async def staging_export(
        deployed_commit: str = "main",
        **kwargs: Any,
    ) -> dict:
        """Transfer anonymized DB from kozak_demo → staging server.

        1. pg_dump odoo_staging з kozak_demo → /tmp/anon_transfer.sql.zst
        2. SFTP стримінг kozak_demo → staging (2MB chunks)
        3. staging: зупинити odoo19, замінити odoo19 DB в odoo19-db
        4. Reset staging code to the prod deployed commit only after DB restore
        5. Публікує msg_deploy_trigger → deploy pipeline з git робить -u all
        6. Cleanup
        """
        staging_lock.acquire()  # re-acquire in case worker restarted mid-pipeline
        logger.info("staging-export: exporting anonymized DB to %s", staging.host)
        web_stopped = False
        db_mutation_started = False
        lock_released = False
        try:
            # 1. Дамп анонімізованої БД на козак_демо
            logger.info("staging-export: dumping anonymized DB on kozak_demo")
            await ssh.run(
                kozak,
                f"set -o pipefail; docker exec {KOZAK_STAGING_DB_CTR}"
                f" pg_dump -U {KOZAK_PG_USER} {KOZAK_STAGING_DB} | zstd -T0 > {TRANSFER_PATH}",
                timeout=1800,
                check=True,
            )
            check = await ssh.run(kozak, f"du -h {TRANSFER_PATH}", check=True)
            logger.info("staging-export: dump size — %s", check.stdout.strip().split()[0])

            # 2. SFTP стримінг kozak → staging
            logger.info("staging-export: streaming to %s", staging.host)
            await _stream_file(
                src_host=kozak.host, src_path=TRANSFER_PATH,
                dst_host=staging.host, dst_path=TRANSFER_PATH,
                key_path=config.ssh_key_path,
            )

            # 3. Замінюємо DB на staging
            logger.info("staging-export: replacing %s DB on %s", STG_DB, staging.host)

            # Зупиняємо web. Odoo can need >30s for graceful worker shutdown.
            await _stop_container_for_db_replace(ssh, staging, STG_CTR)
            web_stopped = True

            # Скидаємо всі з'єднання і дропаємо БД
            await ssh.run(
                staging,
                f"docker exec {STG_DB_CTR} psql -U {STG_PG_USER} -c "
                f"\"SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                f"WHERE datname='{STG_DB}' AND pid <> pg_backend_pid();\"",
                timeout=15,
                check=False,
            )
            db_mutation_started = True
            await ssh.run(
                staging,
                f"docker exec {STG_DB_CTR} dropdb -U {STG_PG_USER} --if-exists {STG_DB}",
                timeout=30,
                check=True,
            )
            await ssh.run(
                staging,
                f"docker exec {STG_DB_CTR} createdb -U {STG_PG_USER} {STG_DB}",
                timeout=30,
                check=True,
            )

            # Відновлення
            logger.info("staging-export: restoring DB (15 min)...")
            await ssh.run(
                staging,
                f"zstd -d < {TRANSFER_PATH} "
                f"| docker exec -i {STG_DB_CTR} psql -U {STG_PG_USER} -d {STG_DB} -q",
                timeout=1800,
                check=True,
            )
            logger.info("staging-export: restore done")

            await _reset_staging_code_to_deployed_commit(
                ssh,
                staging,
                config,
                deployed_commit,
            )

            # Скидаємо deploy-state щоб deploy pipeline завжди запускав після синку БД
            # (без цього git-pull бачить has_changes=false якщо staging гілка не змінилась).
            await ssh.run(
                staging,
                f"rm -f {staging.repo_dir}/.deploy-state/deploy_state_staging",
                check=False,
            )
            logger.info("staging-export: cleared deploy-state for post-sync deploy")

            # Запускаємо web — модульні оновлення виконає deploy pipeline з git
            await ssh.run(staging, f"docker start {STG_CTR}", timeout=30, check=True)
            web_stopped = False
            logger.info("staging-export: staging ready at %s:8069", staging.host)

            # Тригер деплою — deploy pipeline з git зробить -u all з актуального коду
            auth = ZeebeAuthConfig(
                gateway_address=config.zeebe.gateway_address,
                client_id=config.zeebe.client_id,
                client_secret=config.zeebe.client_secret,
                token_url=config.zeebe.token_url,
                audience=config.zeebe.audience,
                use_tls=config.zeebe.use_tls,
            )
            async with zeebe_client(auth) as zeebe:
                await asyncio.wait_for(
                    zeebe.publish_message(
                        name="msg_deploy_trigger",
                        correlation_key="staging",
                        variables={
                            "server_host": "staging",
                            "ssh_user": staging.ssh_user,
                            "repo_dir": staging.repo_dir,
                            "db_name": staging.db_name,
                            "container": staging.container,
                            "branch": "staging",
                            "force_rebuild": True,
                            "force_update_all": True,
                            "staging_sync_deploy": True,
                        },
                        time_to_live_in_milliseconds=300_000,
                    ),
                    timeout=30,
                )
            lock_released = True  # ownership transferred to the post-sync deploy
            logger.info("staging-export: published msg_deploy_trigger for staging deploy pipeline")
            logger.info("staging-export: lock retained until post-sync deploy finishes")

            # 4. Cleanup — зберігаємо зліпок на kozak_demo, видаляємо зі staging
            await ssh.run(
                kozak,
                f"mkdir -p $(dirname {ANON_SNAPSHOT}) && mv -f {TRANSFER_PATH} {ANON_SNAPSHOT}",
                check=False,
            )
            logger.info("staging-export: anonymized snapshot saved → kozak_demo:%s", ANON_SNAPSHOT)
            await ssh.run(staging, f"rm -f {TRANSFER_PATH}", check=False)

            return {}
        except Exception as exc:
            if web_stopped and not db_mutation_started:
                await _start_container_best_effort(ssh, staging, STG_CTR)
            safe_error = _redact_secrets(
                str(exc),
                config.github.deploy_pat,
                config.github.enterprise_pat,
                config.github.token,
            )
            await _capture_incident("staging-export", safe_error, kozak.host, config.ssh_key_path)
            raise _staging_bpmn_error(
                StagingExportError(safe_error, variables={"error_type": "infra"}),
                StagingExportError,
            ) from exc
        finally:
            if not lock_released:
                staging_lock.release()
                logger.info("staging-export: lock released — deploys to staging unblocked")

    # ── staging-nfs-deliver ───────────────────────────────────

    @worker.task(task_type="staging-nfs-deliver", timeout_ms=3_600_000, max_jobs_to_activate=1)
    async def staging_nfs_deliver(**kwargs: Any) -> dict:
        """Deliver anonymized DB snapshot to NFS share on 10.1.1.99 (non-critical).

        Runs in parallel with staging-export. Any failure is caught and logged;
        the process always continues.
        """
        today = datetime.now().strftime("%Y-%m-%d")
        dst_filename = f"odoo_anon_{today}.sql.zst"
        dst_path = f"{NFS_DEST_DIR}/{dst_filename}"

        try:
            logger.info("staging-nfs-deliver: dumping anonymized DB on kozak_demo")
            await ssh.run(
                kozak,
                f"set -o pipefail; docker exec {KOZAK_STAGING_DB_CTR}"
                f" pg_dump -U {KOZAK_PG_USER} {KOZAK_STAGING_DB} | zstd -T0 > {NFS_TMP_PATH}",
                timeout=1800,
                check=True,
            )

            logger.info("staging-nfs-deliver: streaming to %s:%s", NFS_HOST, dst_path)
            await _stream_file(
                src_host=kozak.host,
                src_path=NFS_TMP_PATH,
                dst_host=NFS_HOST,
                dst_path=dst_path,
                key_path=config.ssh_key_path,
            )

            logger.info("staging-nfs-deliver: removing old snapshots on %s", NFS_HOST)
            _ssh_kw = dict(
                username="root",
                client_keys=[config.ssh_key_path],
                known_hosts=None,
                connect_timeout=_SFTP_CONNECT_TIMEOUT,
                keepalive_interval=60,
                keepalive_count_max=5,
            )
            async with asyncssh.connect(NFS_HOST, **_ssh_kw) as conn:
                await conn.run(
                    f"find {NFS_DEST_DIR} -name 'odoo_anon_*.sql.zst'"
                    f" ! -name '{dst_filename}' -delete"
                )

            await ssh.run(kozak, f"rm -f {NFS_TMP_PATH}", check=False)
            logger.info("staging-nfs-deliver: done — %s", dst_filename)

        except Exception as exc:
            logger.error("staging-nfs-deliver: FAILED (non-critical) — %s", exc)
            try:
                await ssh.run(kozak, f"rm -f {NFS_TMP_PATH}", check=False)
            except Exception:
                pass

        return {}
