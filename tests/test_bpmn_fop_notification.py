"""BPMN validation tests for FOP limit monitor + notification processes.

Parses XML to verify structure, input mappings, URLs, flow consistency,
and catches common issues like missing variables or wrong endpoints.
"""

from __future__ import annotations

import os
from pathlib import Path
from xml.etree import ElementTree as ET

import pytest

BPMN_DIR = Path(
    os.environ.get("BPMN_DIR", Path(__file__).resolve().parent.parent / "bpmn" / "zmina-fopa-na-terminali")
)

NS = {
    "bpmn": "http://www.omg.org/spec/BPMN/20100524/MODEL",
    "zeebe": "http://camunda.org/schema/zeebe/1.0",
    "bpmndi": "http://www.omg.org/spec/BPMN/20100524/DI",
    "dc": "http://www.omg.org/spec/DD/20100524/DC",
    "di": "http://www.omg.org/spec/DD/20100524/DI",
}

PROD_ODOO_WEBHOOK = "https://o.tut.ua/web/hook/8531324a-2785-48d1-8f4d-ddd66a267d50"
DEV_ODOO_WEBHOOK = "https://odoo.dev.dobrom.com/web/hook/90fdde6b-47f9-44ba-90b2-19559b206bce"
PROD_REPORT_WEBHOOK = "https://o.tut.ua/web/hook/bb450e05-4673-4cc9-9ba3-8674da99dc61"


# ── Helpers ──────────────────────────────────────────────


def _parse(filename: str) -> ET.Element:
    path = BPMN_DIR / filename
    assert path.exists(), f"BPMN file not found: {path}"
    return ET.parse(path).getroot()


def _process(root: ET.Element) -> ET.Element:
    proc = root.find("bpmn:process", NS)
    assert proc is not None
    return proc


def _find_element(proc: ET.Element, tag: str, element_id: str) -> ET.Element:
    for el in proc.findall(f"bpmn:{tag}", NS):
        if el.get("id") == element_id:
            return el
    pytest.fail(f"Element {tag}#{element_id} not found")


def _get_inputs(element: ET.Element) -> dict[str, str]:
    """Extract zeebe:input mappings as {target: source}."""
    inputs = {}
    for inp in element.findall(".//zeebe:input", NS):
        inputs[inp.get("target")] = inp.get("source")
    return inputs


def _get_outputs(element: ET.Element) -> dict[str, str]:
    """Extract zeebe:output mappings as {target: source}."""
    outputs = {}
    for out in element.findall(".//zeebe:output", NS):
        outputs[out.get("target")] = out.get("source")
    return outputs


def _get_task_type(element: ET.Element) -> str | None:
    td = element.find(".//zeebe:taskDefinition", NS)
    return td.get("type") if td is not None else None


def _get_called_process(element: ET.Element) -> str | None:
    ce = element.find(".//zeebe:calledElement", NS)
    return ce.get("processId") if ce is not None else None


def _get_sequence_flows(proc: ET.Element) -> dict[str, dict]:
    """Returns {flow_id: {sourceRef, targetRef, condition}}."""
    flows = {}
    for sf in proc.findall("bpmn:sequenceFlow", NS):
        cond = sf.find("bpmn:conditionExpression", NS)
        flows[sf.get("id")] = {
            "sourceRef": sf.get("sourceRef"),
            "targetRef": sf.get("targetRef"),
            "condition": cond.text if cond is not None else None,
        }
    return flows


def _get_all_urls(proc: ET.Element) -> list[str]:
    """Extract all URL strings from zeebe:input source attributes."""
    urls = []
    for inp in proc.findall(".//zeebe:input", NS):
        src = inp.get("source", "")
        if "http" in src:
            # Extract URL from FEEL expression like '= "https://..."'
            for part in src.split('"'):
                if part.startswith("http"):
                    urls.append(part)
    return urls


# ══════════════════════════════════════════════════════════
#  Monitor (prod): fop-limit-monitor.bpmn
# ══════════════════════════════════════════════════════════


class TestMonitorProd:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.root = _parse("prod/fop-limit-monitor.bpmn")
        self.proc = _process(self.root)

    def test_process_id(self):
        assert self.proc.get("id") == "Process_fop_limit_monitor"

    def test_is_executable(self):
        assert self.proc.get("isExecutable") == "true"

    def test_has_version_tag(self):
        vt = self.proc.find(".//zeebe:versionTag", NS)
        assert vt is not None
        assert vt.get("value")

    def test_timer_start_event_has_cycle(self):
        se = _find_element(self.proc, "startEvent", "StartEvent_timer")
        tc = se.find(".//bpmn:timeCycle", NS)
        assert tc is not None
        assert tc.text, "timeCycle must have a value (ISO R/... or cron)"

    def test_manual_start_event_exists(self):
        _find_element(self.proc, "startEvent", "StartEvent_manual")

    def test_xor_gateway_has_default_flow(self):
        gw = _find_element(self.proc, "exclusiveGateway", "GW_has_critical")
        assert gw.get("default") == "Flow_no_critical"

    def test_condition_on_critical_branch(self):
        flows = _get_sequence_flows(self.proc)
        assert "critical_count" in flows["Flow_yes_critical"]["condition"]

    def test_report_task_uses_prod_webhook(self):
        st = _find_element(self.proc, "serviceTask", "ST_send_report")
        inputs = _get_inputs(st)
        assert PROD_REPORT_WEBHOOK in inputs["url"]

    def test_call_activity_targets_notification_process(self):
        ca = _find_element(self.proc, "callActivity", "CA_fop_notification")
        assert _get_called_process(ca) == "Process_rebvtea"

    def test_call_activity_has_multi_instance(self):
        ca = _find_element(self.proc, "callActivity", "CA_fop_notification")
        mi = ca.find("bpmn:multiInstanceLoopCharacteristics", NS)
        assert mi is not None
        lc = mi.find(".//zeebe:loopCharacteristics", NS)
        assert "critical_fops" in lc.get("inputCollection")
        assert lc.get("inputElement") == "fop"

    def test_call_activity_maps_all_fop_fields(self):
        ca = _find_element(self.proc, "callActivity", "CA_fop_notification")
        inputs = _get_inputs(ca)
        required = [
            "fop_name", "fop_edrpou", "ep_group", "total_income",
            "limit_amount", "income_percent", "days_to_limit",
            "projected_date", "stores", "stores_count", "trend_ratio",
        ]
        for field in required:
            assert field in inputs, f"Missing input mapping: {field}"

    def test_no_dev_urls_in_prod(self):
        urls = _get_all_urls(self.proc)
        for url in urls:
            assert "dev.dobrom" not in url, f"Dev URL found in prod: {url}"


# ══════════════════════════════════════════════════════════
#  Monitor (dev): fop-limit-monitor-dev.bpmn
# ══════════════════════════════════════════════════════════


class TestMonitorDev:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.root = _parse("fop-limit-monitor-dev.bpmn")
        self.proc = _process(self.root)

    def test_process_id_differs_from_prod(self):
        assert self.proc.get("id") == "Process_fop_limit_monitor_dev"

    def test_is_executable(self):
        assert self.proc.get("isExecutable") == "true"

    def test_report_task_uses_dev_webhook(self):
        """Dev monitor sends report, but must use dev webhook (not prod)."""
        for st in self.proc.findall("bpmn:serviceTask", NS):
            if st.get("id") == "ST_send_report":
                inputs = _get_inputs(st)
                assert "dev.dobrom" in inputs.get("url", ""), \
                    "Dev ST_send_report must target dev webhook"

    def test_no_prod_urls(self):
        urls = _get_all_urls(self.proc)
        for url in urls:
            assert "o.tut.ua" not in url, f"Prod URL found in dev: {url}"

    def test_call_activity_passes_process_instance_key(self):
        ca = _find_element(self.proc, "callActivity", "CA_fop_notification")
        inputs = _get_inputs(ca)
        assert "process_instance_key" in inputs, \
            "Call Activity must pass process_instance_key to subprocess"

    def test_call_activity_maps_all_fop_fields(self):
        ca = _find_element(self.proc, "callActivity", "CA_fop_notification")
        inputs = _get_inputs(ca)
        required = [
            "fop_name", "fop_edrpou", "ep_group", "total_income",
            "limit_amount", "income_percent", "days_to_limit",
            "projected_date", "stores", "stores_count", "trend_ratio",
        ]
        for field in required:
            assert field in inputs, f"Missing input mapping: {field}"

    def test_xor_gateway_has_default_flow(self):
        gw = _find_element(self.proc, "exclusiveGateway", "GW_has_critical")
        assert gw.get("default") == "Flow_no_critical"

    def test_flow_consistency(self):
        """Every outgoing ref should have a matching sequenceFlow."""
        flows = _get_sequence_flows(self.proc)
        flow_ids = set(flows.keys())
        for tag in ("startEvent", "exclusiveGateway", "serviceTask",
                     "callActivity", "endEvent"):
            for el in self.proc.findall(f"bpmn:{tag}", NS):
                for out in el.findall("bpmn:outgoing", NS):
                    assert out.text in flow_ids, \
                        f"{el.get('id')} references missing flow {out.text}"
                for inc in el.findall("bpmn:incoming", NS):
                    assert inc.text in flow_ids, \
                        f"{el.get('id')} references missing flow {inc.text}"


# ══════════════════════════════════════════════════════════
#  Notification (prod): Сповіщення про зміну ФОП (prod) з доріжками.bpmn (Process_rebvtea)
# ══════════════════════════════════════════════════════════


class TestNotificationProd:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.root = _parse("prod/Сповіщення про зміну ФОП (prod) з доріжками.bpmn")
        self.proc = _process(self.root)

    def test_process_id(self):
        assert self.proc.get("id") == "Process_rebvtea"

    def test_is_executable(self):
        assert self.proc.get("isExecutable") == "true"

    def test_starts_with_odoo_check_gateway(self):
        """Process must start with XOR checking odoo_task_id (an optional
        normalize scriptTask is allowed between start and gateway)."""
        start = _find_element(self.proc, "startEvent", "StartEvent_1")
        flows = _get_sequence_flows(self.proc)
        gateway_ids = {gw.get("id") for gw in self.proc.findall("bpmn:exclusiveGateway", NS)}

        target_id = flows[start.find("bpmn:outgoing", NS).text]["targetRef"]
        for _ in range(3):  # allow up to a couple of pass-through nodes
            if target_id in gateway_ids:
                break
            out_flows = [f for f in flows.values() if f["sourceRef"] == target_id]
            assert len(out_flows) == 1, \
                f"Expected single outgoing flow from {target_id} on the way to XOR gateway"
            target_id = out_flows[0]["targetRef"]
        else:
            pytest.fail("No XOR gateway reached from StartEvent_1")

        gw = _find_element(self.proc, "exclusiveGateway", target_id)
        assert gw.get("default") is not None, "XOR gateway must have default flow"
        outgoing = [f for f in flows.values() if f["sourceRef"] == target_id]
        conditions = [f["condition"] for f in outgoing if f["condition"]]
        assert any("odoo_task_id" in c for c in conditions), \
            "XOR gateway must check odoo_task_id"

    def test_create_task_uses_prod_webhook(self):
        st = _find_element(self.proc, "serviceTask", "Activity_1cqrobx")
        inputs = _get_inputs(st)
        assert PROD_ODOO_WEBHOOK in inputs["url"]

    def test_create_task_sets_odoo_task_id(self):
        st = _find_element(self.proc, "serviceTask", "Activity_1cqrobx")
        outputs = _get_outputs(st)
        assert "odoo_task_id" in outputs

    def test_all_user_tasks_use_prod_webhook(self):
        for ut in self.proc.findall("bpmn:userTask", NS):
            inputs = _get_inputs(ut)
            if "url" in inputs:
                assert PROD_ODOO_WEBHOOK in inputs["url"], \
                    f"User task {ut.get('id')} uses wrong URL: {inputs['url']}"

    def test_user_tasks_pass_parent_id(self):
        """All user tasks should reference parent_id: odoo_task_id in body."""
        for ut in self.proc.findall("bpmn:userTask", NS):
            inputs = _get_inputs(ut)
            if "body" in inputs:
                assert "parent_id" in inputs["body"] or "odoo_task_id" in inputs["body"], \
                    f"User task {ut.get('id')} missing parent_id in body"

    def test_user_tasks_pass_process_instance_key(self):
        """All user tasks should include process_instance_key in body."""
        for ut in self.proc.findall("bpmn:userTask", NS):
            inputs = _get_inputs(ut)
            if "body" in inputs:
                assert "process_instance_key" in inputs["body"], \
                    f"User task {ut.get('id')} missing process_instance_key in body"

    def test_paid_change_gateway_has_default(self):
        gw = _find_element(self.proc, "exclusiveGateway", "Gateway_0buteek")
        assert gw.get("default") is not None, \
            "Paid change gateway must have a default flow"

    def test_no_dev_urls_in_prod(self):
        urls = _get_all_urls(self.proc)
        for url in urls:
            assert "dev.dobrom" not in url, f"Dev URL in prod: {url}"

    def test_project_id_is_prod(self):
        """Prod uses _id: 524."""
        st = _find_element(self.proc, "serviceTask", "Activity_1cqrobx")
        inputs = _get_inputs(st)
        assert "_id: 524" in inputs["body"] or '"_id": 524' in inputs["body"], \
            "Prod process should use project _id 524"


# ══════════════════════════════════════════════════════════
#  Notification (dev): Сповіщення про зміну ФОП (2).bpmn
# ══════════════════════════════════════════════════════════


class TestNotificationDev:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.root = _parse("Сповіщення про зміну ФОП (2).bpmn")
        self.proc = _process(self.root)

    def test_process_id_same_as_prod(self):
        """Dev uses same process ID — deployed separately to dev Camunda."""
        assert self.proc.get("id") == "Process_0iy2u1a"

    def test_create_task_uses_dev_webhook(self):
        st = _find_element(self.proc, "serviceTask", "Activity_1cqrobx")
        inputs = _get_inputs(st)
        assert DEV_ODOO_WEBHOOK in inputs["url"]

    def test_all_user_tasks_use_dev_webhook(self):
        for ut in self.proc.findall("bpmn:userTask", NS):
            inputs = _get_inputs(ut)
            if "url" in inputs:
                assert DEV_ODOO_WEBHOOK in inputs["url"], \
                    f"User task {ut.get('id')} uses wrong URL: {inputs['url']}"

    def test_no_prod_urls(self):
        urls = _get_all_urls(self.proc)
        for url in urls:
            assert "o.tut.ua" not in url, f"Prod URL in dev: {url}"

    def test_project_id_is_dev(self):
        """Dev uses _id: 236."""
        st = _find_element(self.proc, "serviceTask", "Activity_1cqrobx")
        inputs = _get_inputs(st)
        assert "_id: 236" in inputs["body"] or '"_id": 236' in inputs["body"], \
            "Dev process should use project _id 236"

    def test_user_tasks_pass_process_instance_key(self):
        for ut in self.proc.findall("bpmn:userTask", NS):
            inputs = _get_inputs(ut)
            if "body" in inputs:
                assert "process_instance_key" in inputs["body"], \
                    f"User task {ut.get('id')} missing process_instance_key"


# ══════════════════════════════════════════════════════════
#  Cross-file consistency
# ══════════════════════════════════════════════════════════


class TestCrossProcessConsistency:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.monitor_prod = _process(_parse("prod/fop-limit-monitor.bpmn"))
        self.monitor_dev = _process(_parse("fop-limit-monitor-dev.bpmn"))
        self.notif_prod = _process(_parse("prod/Сповіщення про зміну ФОП (prod) з доріжками.bpmn"))
        self.notif_dev = _process(_parse("Сповіщення про зміну ФОП (2).bpmn"))

    def test_monitor_calls_correct_subprocess(self):
        """Both monitors must call Process_rebvtea."""
        for proc, label in [(self.monitor_prod, "prod"), (self.monitor_dev, "dev")]:
            ca = _find_element(proc, "callActivity", "CA_fop_notification")
            assert _get_called_process(ca) == "Process_rebvtea", \
                f"{label} monitor calls wrong process"

    def test_call_activity_inputs_match_subprocess_needs(self):
        """Variables mapped by Call Activity must cover what subprocess uses in body expressions."""
        ca = _find_element(self.monitor_dev, "callActivity", "CA_fop_notification")
        ca_inputs = set(_get_inputs(ca).keys())

        # Variables that the notification subprocess references
        needed_by_subprocess = {
            "fop_name", "fop_edrpou", "ep_group", "total_income",
            "limit_amount", "income_percent", "days_to_limit",
            "projected_date", "stores", "process_instance_key",
        }
        missing = needed_by_subprocess - ca_inputs
        assert not missing, f"Call Activity missing mappings for: {missing}"

    def test_prod_monitor_also_passes_process_instance_key(self):
        """Prod monitor must pass process_instance_key to Call Activity."""
        ca = _find_element(self.monitor_prod, "callActivity", "CA_fop_notification")
        inputs = _get_inputs(ca)
        assert "process_instance_key" in inputs, \
            "Prod monitor must pass process_instance_key to Call Activity"

    def test_dev_user_tasks_subset_of_prod(self):
        """Prod is the source of truth; every dev user task must exist in prod.
        Prod may have extra tasks (e.g. swim-lane-specific steps)."""
        prod_uts = {ut.get("id") for ut in self.notif_prod.findall("bpmn:userTask", NS)}
        dev_uts = {ut.get("id") for ut in self.notif_dev.findall("bpmn:userTask", NS)}
        missing_in_prod = dev_uts - prod_uts
        assert not missing_in_prod, \
            f"Dev has user tasks not in prod: {missing_in_prod}"

    def test_dev_gateways_subset_of_prod(self):
        prod_gws = {gw.get("id") for gw in self.notif_prod.findall("bpmn:exclusiveGateway", NS)}
        dev_gws = {gw.get("id") for gw in self.notif_dev.findall("bpmn:exclusiveGateway", NS)}
        missing_in_prod = dev_gws - prod_gws
        assert not missing_in_prod, \
            f"Dev has gateways not in prod: {missing_in_prod}"

    def test_dev_output_keys_subset_of_prod(self):
        """Dev's output targets must be a subset of prod's (prod is source of truth;
        prod may add new outputs like tenant_type that dev hasn't caught up to)."""
        for ut_id in ("Activity_1m5lz1c", "Activity_1glu1uv", "Activity_0rhyd10"):
            prod_ut = _find_element(self.notif_prod, "userTask", ut_id)
            dev_ut = _find_element(self.notif_dev, "userTask", ut_id)
            prod_keys = set(_get_outputs(prod_ut).keys())
            dev_keys = set(_get_outputs(dev_ut).keys())
            missing = dev_keys - prod_keys
            assert not missing, \
                f"{ut_id}: dev has outputs not in prod: {missing}"


# ══════════════════════════════════════════════════════════
#  Flow integrity (reachability, dead ends, orphan flows)
# ══════════════════════════════════════════════════════════


class TestFlowIntegrity:
    """Verify that every process has no orphan flows and all nodes are reachable."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.files = {
            "monitor_prod": _process(_parse("prod/fop-limit-monitor.bpmn")),
            "monitor_dev": _process(_parse("fop-limit-monitor-dev.bpmn")),
            "notif_prod": _process(_parse("prod/Сповіщення про зміну ФОП (prod) з доріжками.bpmn")),
            "notif_dev": _process(_parse("Сповіщення про зміну ФОП (2).bpmn")),
        }

    @pytest.mark.parametrize("label", ["monitor_prod", "monitor_dev", "notif_prod", "notif_dev"])
    def test_no_orphan_sequence_flows(self, label):
        """Every sequenceFlow must reference existing source and target elements."""
        proc = self.files[label]
        node_ids = set()
        for tag in ("startEvent", "endEvent", "exclusiveGateway",
                     "parallelGateway", "serviceTask", "scriptTask",
                     "businessRuleTask", "userTask", "callActivity",
                     "intermediateThrowEvent", "intermediateCatchEvent",
                     "boundaryEvent"):
            for el in proc.findall(f"bpmn:{tag}", NS):
                node_ids.add(el.get("id"))
        flows = _get_sequence_flows(proc)
        for flow_id, flow in flows.items():
            assert flow["sourceRef"] in node_ids, \
                f"{label}: flow {flow_id} references missing source {flow['sourceRef']}"
            assert flow["targetRef"] in node_ids, \
                f"{label}: flow {flow_id} references missing target {flow['targetRef']}"

    @pytest.mark.parametrize("label", ["monitor_prod", "monitor_dev", "notif_prod", "notif_dev"])
    def test_all_gateways_have_at_least_two_flows(self, label):
        """Every XOR gateway must have at least 2 outgoing flows."""
        proc = self.files[label]
        flows = _get_sequence_flows(proc)
        for gw in proc.findall("bpmn:exclusiveGateway", NS):
            gw_id = gw.get("id")
            outgoing = [f for f in flows.values() if f["sourceRef"] == gw_id]
            # Merge gateways may have 1 outgoing, but split gateways need 2+
            incoming = [f for f in flows.values() if f["targetRef"] == gw_id]
            if len(incoming) <= 1:  # split gateway
                assert len(outgoing) >= 2, \
                    f"{label}: split gateway {gw_id} has only {len(outgoing)} outgoing flow(s)"

    @pytest.mark.parametrize("label", ["monitor_prod", "monitor_dev", "notif_prod", "notif_dev"])
    def test_end_events_have_no_outgoing(self, label):
        proc = self.files[label]
        for ee in proc.findall("bpmn:endEvent", NS):
            out = ee.findall("bpmn:outgoing", NS)
            assert len(out) == 0, \
                f"{label}: end event {ee.get('id')} has outgoing flows"

    @pytest.mark.parametrize("label", ["monitor_prod", "monitor_dev", "notif_prod", "notif_dev"])
    def test_start_events_have_no_incoming(self, label):
        proc = self.files[label]
        for se in proc.findall("bpmn:startEvent", NS):
            inc = se.findall("bpmn:incoming", NS)
            assert len(inc) == 0, \
                f"{label}: start event {se.get('id')} has incoming flows"


# ══════════════════════════════════════════════════════════
#  Notification: x_studio_camunda_ field naming
# ══════════════════════════════════════════════════════════


class TestOdooFieldNaming:
    """All Odoo custom fields must use x_studio_camunda_ prefix."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.notif_dev = _process(_parse("Сповіщення про зміну ФОП (2).bpmn"))
        self.notif_prod = _process(_parse("prod/Сповіщення про зміну ФОП (prod) з доріжками.bpmn"))

    @pytest.mark.parametrize("label", ["dev", "prod"])
    def test_x_studio_fields_have_correct_prefix(self, label):
        """Any field starting with x_studio_ must be x_studio_camunda_."""
        proc = self.notif_dev if label == "dev" else self.notif_prod
        for inp in proc.findall(".//zeebe:input", NS):
            src = inp.get("source", "")
            # Find x_studio_ references that are NOT x_studio_camunda_
            if "x_studio_" in src:
                parts = src.split("x_studio_")
                for part in parts[1:]:
                    assert part.startswith("camunda_"), \
                        f"Field x_studio_{part.split(',')[0].split('}')[0]} " \
                        f"missing camunda_ prefix in {label}"
        for out in proc.findall(".//zeebe:output", NS):
            src = out.get("source", "")
            if "x_studio_" in src:
                parts = src.split("x_studio_")
                for part in parts[1:]:
                    assert part.startswith("camunda_"), \
                        f"Output field x_studio_{part.split(',')[0].split('}')[0]} " \
                        f"missing camunda_ prefix in {label}"

    @pytest.mark.parametrize("label", ["dev", "prod"])
    def test_create_task_has_bpmn_process_id(self, label):
        """Create task body should include bpmn_process_id for traceability."""
        proc = self.notif_dev if label == "dev" else self.notif_prod
        st = _find_element(proc, "serviceTask", "Activity_1cqrobx")
        inputs = _get_inputs(st)
        assert "bpmn_process_id" in inputs["body"], \
            f"{label}: create task body missing bpmn_process_id"


# ══════════════════════════════════════════════════════════
#  Notification: user task listener configuration
# ══════════════════════════════════════════════════════════


class TestUserTaskListeners:
    """All user tasks must have taskListener with http-request-smart."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.notif_dev = _process(_parse("Сповіщення про зміну ФОП (2).bpmn"))
        self.notif_prod = _process(_parse("prod/Сповіщення про зміну ФОП (prod) з доріжками.bpmn"))

    @pytest.mark.parametrize("label", ["dev", "prod"])
    def test_all_user_tasks_have_creating_listener(self, label):
        """Each user task needs a taskListener eventType='creating' to create Odoo subtask."""
        proc = self.notif_dev if label == "dev" else self.notif_prod
        for ut in proc.findall("bpmn:userTask", NS):
            listeners = ut.findall(".//zeebe:taskListener", NS)
            creating = [l for l in listeners if l.get("eventType") == "creating"]
            assert len(creating) >= 1, \
                f"{label}: user task {ut.get('id')} missing creating taskListener"
            assert creating[0].get("type") == "http-request-smart", \
                f"{label}: user task {ut.get('id')} listener type should be http-request-smart"

    @pytest.mark.parametrize("label", ["dev", "prod"])
    def test_all_user_tasks_are_zeebe_native(self, label):
        """All user tasks should have <zeebe:userTask /> marker."""
        proc = self.notif_dev if label == "dev" else self.notif_prod
        for ut in proc.findall("bpmn:userTask", NS):
            marker = ut.find(".//zeebe:userTask", NS)
            assert marker is not None, \
                f"{label}: user task {ut.get('id')} missing <zeebe:userTask /> marker"
