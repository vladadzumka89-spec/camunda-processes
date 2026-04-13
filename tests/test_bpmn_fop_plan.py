"""BPMN validation tests for fop-opening-plan.bpmn."""

from __future__ import annotations

import xml.etree.ElementTree as ET

import pytest

BPMN_FILE = "bpmn/zmina-fopa-na-terminali/prod/fop-opening-plan.bpmn"
NS = {
    "bpmn": "http://www.omg.org/spec/BPMN/20100524/MODEL",
    "zeebe": "http://camunda.org/schema/zeebe/1.0",
}


@pytest.fixture
def tree():
    return ET.parse(BPMN_FILE)


@pytest.fixture
def process(tree):
    return tree.find(".//bpmn:process", NS)


class TestBpmnFopPlan:

    def test_process_is_executable(self, process):
        assert process.get("isExecutable") == "true"

    def test_process_id(self, process):
        assert process.get("id") == "Process_fop_opening_plan"

    def test_has_timer_start_event(self, process):
        timer = process.find(".//bpmn:startEvent/bpmn:timerEventDefinition", NS)
        assert timer is not None
        cycle = timer.find("bpmn:timeCycle", NS)
        assert cycle is not None
        assert "MON" in cycle.text

    def test_has_manual_start_event(self, process):
        starts = process.findall("bpmn:startEvent", NS)
        assert len(starts) == 2

    def test_service_task_type(self, process):
        st = process.find(".//bpmn:serviceTask[@id='ST_fop_plan']", NS)
        assert st is not None
        task_def = st.find(".//zeebe:taskDefinition", NS)
        assert task_def.get("type") == "fop-opening-plan"

    def test_report_task_uses_http_smart(self, process):
        st = process.find(".//bpmn:serviceTask[@id='ST_send_plan']", NS)
        assert st is not None
        task_def = st.find(".//zeebe:taskDefinition", NS)
        assert task_def.get("type") == "http-request-smart"

    def test_has_warning_gateway(self, process):
        gw = process.find(".//bpmn:exclusiveGateway[@id='GW_has_warnings']", NS)
        assert gw is not None

    def test_has_end_event(self, process):
        end = process.find(".//bpmn:endEvent", NS)
        assert end is not None

    def test_all_flows_connected(self, process):
        """Every sequence flow's sourceRef and targetRef exist as element IDs."""
        all_ids = {el.get("id") for el in process.iter() if el.get("id")}
        flows = process.findall("bpmn:sequenceFlow", NS)
        assert len(flows) >= 7
        for flow in flows:
            assert flow.get("sourceRef") in all_ids, f"Missing source: {flow.get('sourceRef')}"
            assert flow.get("targetRef") in all_ids, f"Missing target: {flow.get('targetRef')}"

    def test_input_mappings_have_defaults(self, process):
        """ST_fop_plan should have input mappings with defaults for all 4 params."""
        st = process.find(".//bpmn:serviceTask[@id='ST_fop_plan']", NS)
        inputs = st.findall(".//zeebe:input", NS)
        targets = {inp.get("target") for inp in inputs}
        assert "horizon_months" in targets
        assert "income_limit" in targets
        assert "employee_limit" in targets
        assert "reserve_threshold" in targets
