"""Audit analysis handler — deep static conflict analysis after upstream sync.

Integrates the logic from /opt/odoo-enterprise/scripts/audit/analyze.py
into a Camunda job type that runs on the remote server via SSH.

Job type: audit-analysis
"""

from __future__ import annotations

import json
import logging
import textwrap
from typing import Any

from pyzeebe import ZeebeWorker

from ..config import AppConfig
from ..ssh import AsyncSSHClient

logger = logging.getLogger(__name__)

# Isolated workspace path (same as sync.py)
WORKSPACE = "/tmp/sync-workspace"

# Self-contained analysis script transferred to the remote server.
# Inlines SuperCallAnalyzer to avoid external dependencies.
_ANALYSIS_SCRIPT = textwrap.dedent(r'''
import ast
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass, field, asdict
from typing import Any


# === SuperCallAnalyzer (inlined from registry_of_truth) ===

class SuperCallAnalyzer(ast.NodeVisitor):
    """Analyze super() calls in a method, ignoring nested functions."""

    def __init__(self) -> None:
        self.has_super = False
        self.super_in_conditional = False
        self._in_conditional = False

    def _is_super_call(self, node: ast.Call) -> bool:
        func = node.func
        if isinstance(func, ast.Name) and func.id == "super":
            return True
        if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Call):
            return self._is_super_call(func.value)
        return False

    def visit_Call(self, node: ast.Call) -> None:
        if self._is_super_call(node):
            self.has_super = True
            if self._in_conditional:
                self.super_in_conditional = True
        self.generic_visit(node)

    def visit_If(self, node: ast.If) -> None:
        old = self._in_conditional
        self._in_conditional = True
        self.generic_visit(node)
        self._in_conditional = old

    def visit_Try(self, node: ast.Try) -> None:
        old = self._in_conditional
        self._in_conditional = True
        self.generic_visit(node)
        self._in_conditional = old

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        pass  # Skip nested functions

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        pass

    def visit_Lambda(self, node: ast.Lambda) -> None:
        pass


# === Data Models ===

@dataclass
class Conflict:
    id: int = 0
    severity: str = "info"  # critical, warning, info
    type: str = ""  # python_override, js_patch, xml_xpath
    custom_module: str = ""
    custom_file: str = ""
    target: str = ""  # model.method, component, inherit_id
    detail: str = ""
    has_super: bool = False
    base_module: str = ""
    base_file: str = ""
    base_change: str = ""  # modified, removed


# === Git helpers ===

def run_git(root: str, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", root, *args],
        capture_output=True, text=True, timeout=60,
    )
    return result.stdout.strip()


def get_changed_base_files(root: str) -> list[str]:
    """Get files changed in workspace (staged + unstaged) under base dirs."""
    # Use git diff against HEAD to see what sync changed
    output = run_git(root, "diff", "--name-only", "HEAD", "--", "src/community/", "src/enterprise/")
    if not output:
        # Also check untracked
        output = run_git(root, "diff", "--name-only", "--cached", "--", "src/community/", "src/enterprise/")
    if not output:
        return []
    return [f for f in output.splitlines() if "/i18n/" not in f and "/l10n_" not in f]


def get_file_content(root: str, filepath: str) -> str | None:
    full = os.path.join(root, filepath)
    if os.path.exists(full):
        try:
            with open(full, encoding="utf-8", errors="replace") as f:
                return f.read()
        except OSError:
            return None
    return None


# === Python Analysis ===

def extract_python_overrides(filepath: str, module_name: str) -> list[dict]:
    """Extract _inherit method overrides from a Python file."""
    try:
        with open(filepath, encoding="utf-8", errors="replace") as f:
            source = f.read()
        tree = ast.parse(source)
    except (OSError, SyntaxError):
        return []

    results = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue

        inherit_model = ""
        for stmt in node.body:
            if isinstance(stmt, ast.Assign):
                for target in stmt.targets:
                    if isinstance(target, ast.Name) and target.id == "_inherit":
                        if isinstance(stmt.value, ast.Constant) and isinstance(stmt.value.value, str):
                            inherit_model = stmt.value.value
                        elif isinstance(stmt.value, (ast.List, ast.Tuple)):
                            for elt in stmt.value.elts:
                                if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                                    inherit_model = elt.value
                                    break

        if not inherit_model:
            continue

        for item in node.body:
            if not isinstance(item, ast.FunctionDef):
                continue

            analyzer = SuperCallAnalyzer()
            for stmt in item.body:
                analyzer.visit(stmt)

            results.append({
                "model": inherit_model,
                "method": item.name,
                "has_super": analyzer.has_super,
                "super_conditional": analyzer.super_in_conditional and analyzer.has_super,
                "line": item.lineno,
            })

    return results


def extract_base_methods(source: str) -> dict[str, set[str]]:
    """Extract {ClassName.method} from base Python source."""
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return {}

    methods: dict[str, set[str]] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            model = ""
            for stmt in node.body:
                if isinstance(stmt, ast.Assign):
                    for t in stmt.targets:
                        if isinstance(t, ast.Name) and t.id in ("_name", "_inherit"):
                            if isinstance(stmt.value, ast.Constant) and isinstance(stmt.value.value, str):
                                model = stmt.value.value
            if model:
                for item in node.body:
                    if isinstance(item, ast.FunctionDef):
                        methods.setdefault(model, set()).add(item.name)
    return methods


# === JS Analysis ===

PATCH_RE = re.compile(r'patch\(\s*(\w+)(?:\.prototype)?\s*,', re.MULTILINE)
IMPORT_RE = re.compile(r'import\s*\{([^}]+)\}\s*from\s*["\'](@[\w/.@-]+)["\']', re.MULTILINE)
ODOO_MODULE_RE = re.compile(r'@([\w-]+)/(.*)')


def extract_js_patches(filepath: str, module_name: str) -> list[dict]:
    """Extract patch() targets from a JS file."""
    try:
        with open(filepath, encoding="utf-8", errors="replace") as f:
            source = f.read()
    except OSError:
        return []

    imports: dict[str, str] = {}
    for m in IMPORT_RE.finditer(source):
        names = [n.strip().split(" as ")[0].strip() for n in m.group(1).split(",")]
        for name in names:
            if name:
                imports[name] = m.group(2)

    results = []
    for m in PATCH_RE.finditer(source):
        target = m.group(1)
        import_path = imports.get(target, "")
        base_module = ""
        if import_path:
            pm = ODOO_MODULE_RE.match(import_path)
            if pm:
                base_module = pm.group(1)

        results.append({
            "target": target,
            "import_path": import_path,
            "base_module": base_module,
            "line": source[:m.start()].count("\n") + 1,
        })

    return results


# === XML Analysis ===

INHERIT_RE = re.compile(
    r'(?:inherit_id\s*=\s*["\']([^"\']+)["\']'
    r'|<field\s+name=["\']inherit_id["\']\s+ref=["\']([^"\']+)["\'])',
    re.MULTILINE,
)
XPATH_RE = re.compile(r'<xpath\s+expr=["\']([^"\']+)["\']', re.MULTILINE)


def extract_xml_inherits(filepath: str, module_name: str) -> list[dict]:
    """Extract inherit_id + xpath from XML view files."""
    try:
        with open(filepath, encoding="utf-8", errors="replace") as f:
            content = f.read()
    except OSError:
        return []

    raw = INHERIT_RE.findall(content)
    inherit_ids = list(set(g1 or g2 for g1, g2 in raw if g1 or g2))
    xpaths = XPATH_RE.findall(content)

    results = []
    for iid in inherit_ids:
        base_module = iid.split(".")[0] if "." in iid else ""
        for xpath in xpaths:
            results.append({
                "inherit_id": iid,
                "xpath": xpath,
                "base_module": base_module,
            })
        if not xpaths:
            results.append({
                "inherit_id": iid,
                "xpath": "",
                "base_module": base_module,
            })

    return results


# === Main Analysis ===

def discover_custom_modules(src_root: str) -> dict[str, str]:
    """Discover custom modules: {name: path}."""
    modules = {}
    for source_dir in ["custom", "third-party"]:
        base = os.path.join(src_root, source_dir)
        if not os.path.isdir(base):
            continue
        for name in os.listdir(base):
            mod_path = os.path.join(base, name)
            if not os.path.isdir(mod_path) or name.startswith(".") or name == "__pycache__":
                continue
            if os.path.exists(os.path.join(mod_path, "__manifest__.py")):
                modules[name] = mod_path
    return modules


def run_analysis(workspace: str) -> dict[str, Any]:
    src_root = os.path.join(workspace, "src")
    changed_files = get_changed_base_files(workspace)
    changed_set = set(changed_files)

    py_changed = [f for f in changed_files if f.endswith(".py")]
    js_changed = [f for f in changed_files if f.endswith(".js")]
    xml_changed = [f for f in changed_files if f.endswith(".xml")]

    if not changed_files:
        return {"conflicts": [], "stats": {"total": 0}, "extension_points": 0}

    # Build model → changed base files index
    model_to_files: dict[str, list[str]] = {}
    for f in py_changed:
        if "/models/" not in f or f.endswith("__init__.py"):
            continue
        content = get_file_content(workspace, f)
        if not content:
            continue
        methods_by_model = extract_base_methods(content)
        for model in methods_by_model:
            model_to_files.setdefault(model, []).append(f)

    # Build changed base module set (for JS/XML matching)
    changed_base_modules: set[str] = set()
    for f in changed_files:
        parts = f.split("/")
        if len(parts) >= 3 and parts[0] == "src" and parts[1] in ("community", "enterprise"):
            # src/enterprise/MODULE/... or src/community/odoo/addons/MODULE/...
            if parts[1] == "enterprise" and len(parts) >= 3:
                changed_base_modules.add(parts[2])
            elif parts[1] == "community" and len(parts) >= 5 and parts[3] == "addons":
                changed_base_modules.add(parts[4])

    # Discover custom modules
    modules = discover_custom_modules(src_root)
    conflicts: list[dict] = []
    total_ext_points = 0

    for mod_name, mod_path in sorted(modules.items()):
        for root, _, files in os.walk(mod_path):
            for fname in files:
                fpath = os.path.join(root, fname)
                rel_path = os.path.relpath(fpath, workspace)

                if fname.endswith(".py") and fname != "__init__.py":
                    overrides = extract_python_overrides(fpath, mod_name)
                    total_ext_points += len(overrides)

                    for ov in overrides:
                        model = ov["model"]
                        method = ov["method"]
                        base_files = model_to_files.get(model, [])
                        if not base_files:
                            continue

                        for bf in base_files:
                            content = get_file_content(workspace, bf)
                            if not content:
                                continue
                            base_methods = extract_base_methods(content)
                            model_methods = base_methods.get(model, set())
                            if method not in model_methods:
                                continue

                            if not ov["has_super"]:
                                severity = "critical"
                            elif ov["super_conditional"]:
                                severity = "warning"
                            else:
                                severity = "info"

                            conflicts.append({
                                "type": "python_override",
                                "severity": severity,
                                "custom_module": mod_name,
                                "custom_file": rel_path,
                                "target": f"{model}.{method}",
                                "has_super": ov["has_super"],
                                "super_conditional": ov.get("super_conditional", False),
                                "base_file": bf,
                                "line": ov["line"],
                            })

                elif fname.endswith(".js"):
                    patches = extract_js_patches(fpath, mod_name)
                    total_ext_points += len(patches)

                    for patch in patches:
                        if patch["base_module"] and patch["base_module"] in changed_base_modules:
                            conflicts.append({
                                "type": "js_patch",
                                "severity": "warning",
                                "custom_module": mod_name,
                                "custom_file": rel_path,
                                "target": patch["target"],
                                "base_module": patch["base_module"],
                                "line": patch["line"],
                            })

                elif fname.endswith(".xml") and "/views/" in rel_path:
                    inherits = extract_xml_inherits(fpath, mod_name)
                    total_ext_points += len(inherits)

                    for inh in inherits:
                        if inh["base_module"] and inh["base_module"] in changed_base_modules:
                            conflicts.append({
                                "type": "xml_xpath",
                                "severity": "warning",
                                "custom_module": mod_name,
                                "custom_file": rel_path,
                                "target": inh["inherit_id"],
                                "xpath": inh.get("xpath", ""),
                                "base_module": inh["base_module"],
                            })

    # Sort by severity
    sev_order = {"critical": 0, "warning": 1, "info": 2}
    conflicts.sort(key=lambda c: sev_order.get(c.get("severity", "info"), 3))
    for i, c in enumerate(conflicts, 1):
        c["id"] = i

    stats = {
        "total": len(conflicts),
        "critical": sum(1 for c in conflicts if c.get("severity") == "critical"),
        "warning": sum(1 for c in conflicts if c.get("severity") == "warning"),
        "info": sum(1 for c in conflicts if c.get("severity") == "info"),
        "by_type": {
            "python": sum(1 for c in conflicts if c.get("type") == "python_override"),
            "js": sum(1 for c in conflicts if c.get("type") == "js_patch"),
            "xml": sum(1 for c in conflicts if c.get("type") == "xml_xpath"),
        },
        "base_files_changed": len(changed_files),
        "custom_modules_scanned": len(modules),
    }

    return {
        "conflicts": conflicts,
        "stats": stats,
        "extension_points": total_ext_points,
    }


if __name__ == "__main__":
    workspace = sys.argv[1] if len(sys.argv) > 1 else "/tmp/sync-workspace"
    result = run_analysis(workspace)
    json.dump(result, sys.stdout, ensure_ascii=False)
''').strip()


def register_audit_handlers(
    worker: ZeebeWorker,
    config: AppConfig,
    ssh: AsyncSSHClient,
) -> None:
    """Register audit analysis task handlers."""

    def _resolve_server(server_host: str = ""):
        return config.resolve_server(server_host or "kozak_demo")

    # ── audit-analysis ────────────────────────────────────────

    @worker.task(task_type="audit-analysis", timeout_ms=300_000)
    async def audit_analysis(
        changed_modules: str = "",
        server_host: str = "",
        **kwargs: Any,
    ) -> dict:
        """Deep static analysis: Python overrides, JS patches, XML xpaths.

        Transfers a self-contained analysis script to the remote workspace,
        runs it, and returns structured conflict data.
        """
        server = _resolve_server(server_host)

        if not changed_modules:
            return {
                "audit_conflicts": 0,
                "audit_critical": 0,
                "audit_warning": 0,
                "audit_report": "",
            }

        # Transfer analysis script to workspace
        # Use heredoc to write the script via SSH
        script_path = f"{WORKSPACE}/_audit_analyze.py"
        await ssh.run(
            server,
            f"cat > {script_path} << 'AUDIT_SCRIPT_EOF'\n{_ANALYSIS_SCRIPT}\nAUDIT_SCRIPT_EOF",
            check=True,
            timeout=30,
        )

        # Stage files for diff detection (git add -N to track new files)
        await ssh.run(
            server,
            f"cd {WORKSPACE} && git add -N src/community/ src/enterprise/ 2>/dev/null || true",
            timeout=30,
        )

        # Run the analysis script
        result = await ssh.run(
            server,
            f"cd {WORKSPACE} && python3 {script_path} {WORKSPACE} 2>/dev/null",
            timeout=240,
        )

        # Cleanup script
        await ssh.run(server, f"rm -f {script_path}", timeout=10)

        if not result.success or not result.stdout.strip():
            logger.warning(
                "audit-analysis returned no output (exit=%d): %s",
                result.exit_code, result.stderr[:200] if result.stderr else "",
            )
            return {
                "audit_conflicts": 0,
                "audit_critical": 0,
                "audit_warning": 0,
                "audit_report": "",
            }

        # Parse JSON output
        try:
            data = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            logger.error("audit-analysis JSON parse error: %s", exc)
            return {
                "audit_conflicts": 0,
                "audit_critical": 0,
                "audit_warning": 0,
                "audit_report": f"JSON parse error: {exc}",
            }

        conflicts = data.get("conflicts", [])
        stats = data.get("stats", {})
        ext_points = data.get("extension_points", 0)

        # Build markdown report
        report_lines = [
            "## Audit Analysis Report",
            "",
            f"**Extension points scanned:** {ext_points}",
            f"**Conflicts found:** {stats.get('total', 0)}",
            f"  - Critical: {stats.get('critical', 0)}",
            f"  - Warning: {stats.get('warning', 0)}",
            f"  - Info: {stats.get('info', 0)}",
            "",
        ]

        if conflicts:
            report_lines.append("| # | Severity | Type | Custom Module | Target | Base | File | Line | Super |")
            report_lines.append("|---|---|---|---|---|---|---|---|---|")

            for c in conflicts[:80]:  # Limit to 80 in report
                sev_icon = {"critical": "!!!", "warning": "!", "info": "-"}.get(
                    c.get("severity", "info"), "-"
                )
                custom_file = c.get("custom_file", "")
                line_no = c.get("line", "")
                # Super call info for python overrides
                if c.get("type") == "python_override":
                    if not c.get("has_super"):
                        super_info = "no"
                    elif c.get("super_conditional"):
                        super_info = "cond"
                    else:
                        super_info = "yes"
                elif c.get("type") == "xml_xpath":
                    super_info = c.get("xpath", "")[:40]
                else:
                    super_info = ""
                report_lines.append(
                    f"| {c.get('id', '')} | {sev_icon} {c.get('severity', '')} "
                    f"| {c.get('type', '')} | {c.get('custom_module', '')} "
                    f"| {c.get('target', '')} | {c.get('base_file', c.get('base_module', ''))} "
                    f"| {custom_file} | {line_no} | {super_info} |"
                )

            if len(conflicts) > 80:
                report_lines.append(f"| ... | ... | ... | +{len(conflicts) - 80} more | ... | ... | ... | ... | ... |")

        audit_report = "\n".join(report_lines)

        logger.info(
            "audit-analysis: %d conflicts (%d critical, %d warning), %d extension points",
            stats.get("total", 0),
            stats.get("critical", 0),
            stats.get("warning", 0),
            ext_points,
        )

        return {
            "audit_conflicts": stats.get("total", 0),
            "audit_critical": stats.get("critical", 0),
            "audit_warning": stats.get("warning", 0),
            "audit_report": audit_report,
        }
