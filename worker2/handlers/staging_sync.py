"""Nightly staging sync handlers — dump prod DB, anonymize, transfer to staging.

Pipeline:
  1. staging-dump:      prod: pg_dump odoo19 | zstd → /tmp/dump.zst на козак_демо
  2. staging-anonymize: kozak_demo: sync.sh deploy → анонімізована БД в kozak-staging контейнері
  3. staging-export:    kozak pg_dump → SFTP → staging: замінює odoo19 DB напряму в odoo19-db
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import asyncssh
from pyzeebe import ZeebeClient, ZeebeWorker

from ..auth import ZeebeAuthConfig, create_channel
from ..config import AppConfig
from ..ssh import AsyncSSHClient
from .. import staging_lock

logger = logging.getLogger(__name__)

DUMP_PATH      = "/tmp/nightly_staging_dump.sql.zst"
TRANSFER_PATH  = "/tmp/anon_transfer.sql.zst"
ANON_SNAPSHOT  = "/opt/odoo-enterprise/backups/anon_latest.sql.zst"
SYNC_SCRIPT    = "/opt/odoo-enterprise/scripts/db_anonymize/sync.sh"
SYNC_LOG_PATH  = "/opt/odoo-enterprise/scripts/db_anonymize/sync.log"
INCIDENTS_FILE = Path("/app/staging-sync-incidents.md")

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


_SFTP_CHUNK_TIMEOUT = 120   # seconds per 2MB chunk read/write
_SFTP_CONNECT_TIMEOUT = 30  # seconds for SSH handshake


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
        f"docker exec odoo19-db pg_dump -U odoo odoo19 | zstd -T0 > {prod_tmp}",
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

    # ── staging-anonymize ─────────────────────────────────────

    @worker.task(task_type="staging-anonymize", timeout_ms=7_200_000, max_jobs_to_activate=1)
    async def staging_anonymize(
        dump_path: str = DUMP_PATH,
        deployed_commit: str = "main",
        **kwargs: Any,
    ) -> dict:
        """Run sync.sh deploy on kozak_demo: restore → anonymize → local staging."""
        try:
            logger.info("staging-anonymize: syncing src/ scripts/ docker-compose.staging.yml on kozak_demo to origin/main")
            await ssh.run(
                kozak,
                f"git -C /opt/odoo-enterprise fetch origin main "
                f"&& git -C /opt/odoo-enterprise checkout refs/remotes/origin/main -- src/ scripts/ docker-compose.staging.yml",
                timeout=120,
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
            await _capture_incident("staging-anonymize", str(exc), kozak.host, config.ssh_key_path)
            raise

    # ── staging-export ────────────────────────────────────────

    @worker.task(task_type="staging-export", timeout_ms=7_200_000, max_jobs_to_activate=1)
    async def staging_export(**kwargs: Any) -> dict:
        """Transfer anonymized DB from kozak_demo → staging server.

        1. pg_dump odoo_staging з kozak_demo → /tmp/anon_transfer.sql.zst
        2. SFTP стримінг kozak_demo → staging (2MB chunks)
        3. staging: зупинити odoo19, замінити odoo19 DB в odoo19-db, запустити
        4. Публікує msg_deploy_trigger → deploy pipeline з git робить -u all
        5. Cleanup
        """
        staging_lock.acquire()  # re-acquire in case worker restarted mid-pipeline
        logger.info("staging-export: exporting anonymized DB to %s", staging.host)
        try:
            # 1. Дамп анонімізованої БД на козак_демо
            logger.info("staging-export: dumping anonymized DB on kozak_demo")
            await ssh.run(
                kozak,
                f"docker exec {KOZAK_STAGING_DB_CTR} pg_dump -U {KOZAK_PG_USER} {KOZAK_STAGING_DB} "
                f"| zstd -T0 > {TRANSFER_PATH}",
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

            # Зупиняємо web
            await ssh.run(staging, f"docker stop {STG_CTR} 2>/dev/null || true", timeout=30)

            # Скидаємо всі з'єднання і дропаємо БД
            await ssh.run(
                staging,
                f"docker exec {STG_DB_CTR} psql -U {STG_PG_USER} -c "
                f"\"SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                f"WHERE datname='{STG_DB}' AND pid <> pg_backend_pid();\"",
                timeout=15,
                check=False,
            )
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

            # Скидаємо deploy-state щоб deploy pipeline завжди запускав -u all після синку БД
            # (без цього git-pull бачить has_changes=false якщо staging гілка не змінилась
            # і пропускає оновлення модулів — нова БД від прод залишається без нових колонок)
            await ssh.run(
                staging,
                f"rm -f {staging.repo_dir}/.deploy-state/deploy_state_staging",
                check=False,
            )
            logger.info("staging-export: cleared deploy-state to force -u all on next deploy")

            # Запускаємо web — модульні оновлення виконає deploy pipeline з git
            await ssh.run(staging, f"docker start {STG_CTR}", timeout=30, check=True)
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
            zeebe = ZeebeClient(create_channel(auth))
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
                    },
                    time_to_live_in_milliseconds=300_000,
                ),
                timeout=30,
            )
            logger.info("staging-export: published msg_deploy_trigger for staging deploy pipeline")

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
            await _capture_incident("staging-export", str(exc), kozak.host, config.ssh_key_path)
            raise
        finally:
            staging_lock.release()
            logger.info("staging-export: lock released — deploys to staging unblocked")
