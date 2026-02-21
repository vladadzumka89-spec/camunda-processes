"""Clickbot test handler — 1 task type.

Source: .github/workflows/clickbot_tests.yml
Runs E2E browser tests in an isolated environment.
"""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Any

from pyzeebe import ZeebeWorker

from ..config import AppConfig
from ..ssh import AsyncSSHClient

logger = logging.getLogger(__name__)


def register_clickbot_handlers(
    worker: ZeebeWorker,
    config: AppConfig,
    ssh: AsyncSSHClient,
) -> None:
    """Register clickbot test handler."""

    @worker.task(task_type="clickbot-test", timeout_ms=3_600_000)
    async def clickbot_test(
        server_host: str = "",
        db_name: str = "",
        test_mode: str = "light",
        **kwargs: Any,
    ) -> dict:
        """Run clickbot E2E tests in isolated environment.

        Steps:
        1. pg_dump production DB
        2. Start isolated clickbot-db (tmpfs PostgreSQL)
        3. Restore dump
        4. Prepare DB (admin user, disable crons, clear assets)
        5. Run clickbot tests
        6. Parse results
        7. Cleanup
        """
        server_name = server_host or "staging"
        server = config.resolve_server(server_name)
        db = db_name or server.db_name
        ctr = server.container
        repo = server.repo_dir

        try:
            # 1. Cleanup previous runs
            await ssh.run_in_repo(
                server,
                "docker compose -f docker-compose.clickbot.yml down -v 2>/dev/null || true",
            )

            # 2. Dump production DB
            logger.info("Dumping production DB %s on %s", db, server.host)
            await ssh.run(
                server,
                f"docker exec {ctr}-db pg_dump -U odoo -Fc --no-owner --no-acl {db} "
                f"> /tmp/clickbot_db_dump.custom",
                check=True,
                timeout=300,
            )

            # 3. Start clickbot DB
            await ssh.run_in_repo(
                server,
                "docker compose -f docker-compose.clickbot.yml up -d clickbot-db",
                check=True,
            )
            # Wait for DB to be ready
            await asyncio.sleep(5)

            # 4. Restore dump
            await ssh.run(
                server,
                "docker cp /tmp/clickbot_db_dump.custom clickbot-test-db:/tmp/dump.custom",
                check=True,
            )
            await ssh.run(
                server,
                "docker exec clickbot-test-db pg_restore -U clickbot -d postgres "
                "--no-owner --no-acl --create /tmp/dump.custom 2>/dev/null || true",
                timeout=300,
            )

            # Verify DB exists
            verify = await ssh.run(
                server,
                f"docker exec clickbot-test-db psql -U clickbot -d postgres -tc "
                f"\"SELECT 1 FROM pg_database WHERE datname = '{db}'\" | grep -q 1",
            )
            if not verify.success:
                raise RuntimeError(f"Database {db} was not restored")

            # 5. Prepare DB: neutralize crons/mail, clear assets
            prepare_sql = (
                "UPDATE ir_cron SET active = false; "
                "UPDATE fetchmail_server SET active = false WHERE active = true; "
                "UPDATE ir_mail_server SET active = false WHERE active = true; "
                "DELETE FROM ir_attachment WHERE url LIKE '/web/assets/%';"
            )
            await ssh.run(
                server,
                f"docker exec clickbot-test-db psql -U clickbot -d {db} -c \"{prepare_sql}\"",
                check=True,
            )

            # 6. Run clickbot tests
            test_tag = "tut_clickbot_full" if test_mode == "full" else "tut_clickbot"
            test_timeout = 3000 if test_mode == "full" else 600

            logger.info("Running clickbot tests (mode=%s, tag=%s)", test_mode, test_tag)
            result = await ssh.run_in_repo(
                server,
                f"docker compose -f docker-compose.clickbot.yml "
                f"run --rm -e TEST_MODE={test_mode} clickbot-test",
                timeout=test_timeout + 60,
            )

            # 7. Parse results
            log_output = result.stdout + result.stderr
            passed = log_output.count("clickbot test succeeded")
            failed_matches = re.findall(r"FAIL: Subtest.*?app='([^']+)'", log_output)
            n_failed = len(failed_matches)
            n_skipped = log_output.count("skipped Subtest") + log_output.count("Skipping app without xmlid")

            clickbot_passed = passed > 0 and n_failed == 0 and result.exit_code == 0

            # Build report
            report_lines = [
                f"Mode: {test_mode}",
                f"Total: {passed + n_failed + n_skipped}",
                f"Passed: {passed}",
                f"Failed: {n_failed}",
                f"Skipped: {n_skipped}",
            ]
            if failed_matches:
                report_lines.append("Failed apps: " + ", ".join(failed_matches))

            clickbot_report = "\n".join(report_lines)

            logger.info(
                "Clickbot results: passed=%s (%d ok, %d failed, %d skipped)",
                clickbot_passed, passed, n_failed, n_skipped,
            )
            return {
                "clickbot_passed": clickbot_passed,
                "clickbot_report": clickbot_report,
            }

        finally:
            # Always cleanup
            await ssh.run_in_repo(
                server,
                "docker compose -f docker-compose.clickbot.yml down -v 2>/dev/null || true",
            )
            await ssh.run(server, "rm -f /tmp/clickbot_db_dump.custom")
