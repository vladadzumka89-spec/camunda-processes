"""Clickbot test handler — 1 task type.

Source: .github/workflows/clickbot_tests.yml
Runs E2E browser tests in an isolated environment.
"""

from __future__ import annotations

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
        test_mode: str = "full",
        **kwargs: Any,
    ) -> dict:
        """Run clickbot E2E tests in isolated environment.

        The clickbot-test Docker service (docker-compose.clickbot.yml)
        handles everything: DB restore, module update, browser tests.
        This handler only does pg_dump, launches the container, and parses results.
        """
        server_name = server_host or "staging"
        server = config.resolve_server(server_name)
        db = db_name or server.db_name
        ctr = server.container

        try:
            # 1. Cleanup previous runs
            await ssh.run_in_repo(
                server,
                "docker compose -f docker-compose.clickbot.yml down -v 2>/dev/null || true",
                timeout=300,
            )

            # 2. Dump production DB
            logger.info("Dumping production DB %s on %s", db, server.host)
            await ssh.run(
                server,
                f"docker exec {ctr}-db pg_dump -U odoo -Fc --no-owner --no-acl {db} "
                f"> /tmp/clickbot_db_dump.custom",
                check=True,
                timeout=600,
            )

            # 3. Start clickbot-db, restore dump, rename to clickbot_test
            await ssh.run_in_repo(
                server,
                "docker compose -f docker-compose.clickbot.yml up -d clickbot-db",
                check=True,
            )

            # Wait for DB health check
            await ssh.run(
                server,
                "for i in $(seq 1 30); do "
                "docker exec clickbot-test-db pg_isready -U clickbot && break; "
                "sleep 2; done",
                timeout=120,
            )

            # Copy dump into container and restore
            await ssh.run(
                server,
                "docker cp /tmp/clickbot_db_dump.custom clickbot-test-db:/tmp/dump.custom",
                check=True,
                timeout=120,
            )
            await ssh.run(
                server,
                "docker exec clickbot-test-db pg_restore -U clickbot -d postgres "
                "--no-owner --no-acl --create /tmp/dump.custom 2>/dev/null || true",
                timeout=600,
            )

            # Rename DB to clickbot_test (what the test container expects)
            await ssh.run(
                server,
                f"docker exec clickbot-test-db psql -U clickbot -d postgres -c "
                f"'ALTER DATABASE \"{db}\" RENAME TO clickbot_test'",
                check=True,
                timeout=30,
            )

            # Neutralize crons, mail (keep assets — avoid cold-start failures)
            prepare_sql = (
                "UPDATE ir_cron SET active = false; "
                "UPDATE fetchmail_server SET active = false WHERE active = true; "
                "UPDATE ir_mail_server SET active = false WHERE active = true;"
            )
            await ssh.run(
                server,
                f'docker exec clickbot-test-db psql -U clickbot -d clickbot_test -c "{prepare_sql}"',
                check=True,
                timeout=30,
            )

            # 4. Run clickbot tests via docker compose
            test_timeout = 3600 if test_mode == "full" else 600
            logger.info("Running clickbot tests (mode=%s)", test_mode)
            result = await ssh.run_in_repo(
                server,
                f"docker compose -f docker-compose.clickbot.yml "
                f"run --rm -e TEST_MODE={test_mode} -e DB_DUMP_FILE=skip clickbot-test",
                timeout=test_timeout + 120,
            )

            # 5. Parse results
            log_output = result.stdout + result.stderr

            # Passed apps
            passed_matches = re.findall(
                r"clickbot test succeeded.*?app='([^']+)'", log_output,
            )
            n_passed = len(passed_matches) or log_output.count("clickbot test succeeded")

            # Failed apps with reasons
            failed_details: list[dict[str, str]] = []
            for m in re.finditer(
                r"FAIL: Subtest.*?app='([^']+)'(?:.*?(?:Error|Exception|Traceback)[^\n]*([^\n]{0,200}))?",
                log_output,
                re.DOTALL,
            ):
                app = m.group(1)
                reason = m.group(2).strip() if m.group(2) else ""
                if not reason:
                    # Try to find error context nearby
                    pos = m.end()
                    snippet = log_output[pos:pos + 500]
                    err_match = re.search(r"((?:Error|Exception|AssertionError)[^\n]{0,200})", snippet)
                    reason = err_match.group(1).strip() if err_match else "Невідома помилка"
                failed_details.append({"app": app, "reason": reason})
            n_failed = len(failed_details)

            # Skipped apps with reasons
            skipped_details: list[dict[str, str]] = []
            for m in re.finditer(
                r"(?:skipped Subtest.*?app='([^']+)'|Skipping app without xmlid[:\s]*([^\n]*))",
                log_output,
            ):
                app = m.group(1) or m.group(2) or "unknown"
                skipped_details.append({"app": app.strip(), "reason": "Немає xmlid" if not m.group(1) else "Скіпнуто тестом"})
            n_skipped = len(skipped_details) or (
                log_output.count("skipped Subtest")
                + log_output.count("Skipping app without xmlid")
            )

            clickbot_passed = n_passed > 0 and n_failed == 0 and result.exit_code == 0

            # Build detailed report
            report_lines = [
                f"Mode: {test_mode}",
                f"Total: {n_passed + n_failed + n_skipped}",
                f"Passed: {n_passed}",
                f"Failed: {n_failed}",
                f"Skipped: {n_skipped}",
                "",
            ]

            if passed_matches:
                report_lines.append("✅ Passed apps:")
                for app in passed_matches:
                    report_lines.append(f"  - {app}")
                report_lines.append("")

            if failed_details:
                report_lines.append("❌ Failed apps:")
                for fd in failed_details:
                    report_lines.append(f"  - {fd['app']}: {fd['reason']}")
                report_lines.append("")

            if skipped_details:
                report_lines.append("⏭️ Skipped apps:")
                for sd in skipped_details:
                    report_lines.append(f"  - {sd['app']}: {sd['reason']}")

            clickbot_report = "\n".join(report_lines)

            clickbot_failed_apps = ", ".join(fd["app"] for fd in failed_details) if failed_details else ""
            clickbot_passed_apps = ", ".join(passed_matches) if passed_matches else ""
            clickbot_skipped_apps = ", ".join(sd["app"] for sd in skipped_details) if skipped_details else ""

            logger.info(
                "Clickbot results: passed=%s (%d ok, %d failed, %d skipped)",
                clickbot_passed, n_passed, n_failed, n_skipped,
            )
            return {
                "clickbot_passed": clickbot_passed,
                "clickbot_report": clickbot_report,
                "clickbot_failed_apps": clickbot_failed_apps,
                "clickbot_passed_apps": clickbot_passed_apps,
                "clickbot_skipped_apps": clickbot_skipped_apps,
            }

        finally:
            # Always cleanup
            await ssh.run_in_repo(
                server,
                "docker compose -f docker-compose.clickbot.yml down -v 2>/dev/null || true",
                timeout=300,
            )
            await ssh.run(server, "rm -f /tmp/clickbot_db_dump.custom")
