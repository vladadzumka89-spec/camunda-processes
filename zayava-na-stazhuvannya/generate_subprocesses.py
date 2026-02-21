#!/usr/bin/env python3
"""Generate updated BPMN v2 files for all 3 subprocesses with CLAUDE.md compliance."""

import html

WEBHOOK_URL = "https://o.tut.ua/web/hook/67f62d7c-2612-444c-baf3-ad409c769bbe"
Q = "&quot;"  # XML entity for double quote


def esc(s):
    """Escape XML special chars in attribute values."""
    return html.escape(s, quote=True)


def odoo_start_pattern(process_name):
    """Generate Odoo Start Pattern elements (Rule #1)."""
    return f"""
    <bpmn:exclusiveGateway id="Gateway_odoo_xor" default="Flow_odoo_default">
      <bpmn:incoming>Flow_to_odoo_xor</bpmn:incoming>
      <bpmn:outgoing>Flow_odoo_default</bpmn:outgoing>
      <bpmn:outgoing>Flow_odoo_skip</bpmn:outgoing>
    </bpmn:exclusiveGateway>
    <bpmn:serviceTask id="Activity_create_main_task" name="Створити головне завдання">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="http-request-smart" />
        <zeebe:ioMapping>
          <zeebe:input source="={Q}POST{Q}" target="method" />
          <zeebe:input source="={Q}{esc(WEBHOOK_URL)}{Q}" target="url" />
          <zeebe:input source="={{{Q}Content-Type{Q}:{Q}application/json{Q}}}" target="headers" />
          <zeebe:input source="={{name: {Q}{esc(process_name)}{Q}, create_process: true, _model: {Q}project.project{Q}, _id: 252}}" target="body" />
        </zeebe:ioMapping>
      </bpmn:extensionElements>
      <bpmn:incoming>Flow_odoo_default</bpmn:incoming>
      <bpmn:outgoing>Flow_create_to_merge</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:exclusiveGateway id="Gateway_odoo_merge">
      <bpmn:incoming>Flow_odoo_skip</bpmn:incoming>
      <bpmn:incoming>Flow_create_to_merge</bpmn:incoming>
      <bpmn:outgoing>Flow_merge_to_process</bpmn:outgoing>
    </bpmn:exclusiveGateway>"""


def odoo_start_flows():
    """Generate Odoo Start Pattern sequence flows."""
    return """
    <bpmn:sequenceFlow id="Flow_to_odoo_xor" sourceRef="StartEvent_1" targetRef="Gateway_odoo_xor" />
    <bpmn:sequenceFlow id="Flow_odoo_default" sourceRef="Gateway_odoo_xor" targetRef="Activity_create_main_task" />
    <bpmn:sequenceFlow id="Flow_odoo_skip" sourceRef="Gateway_odoo_xor" targetRef="Gateway_odoo_merge">
      <bpmn:conditionExpression xsi:type="bpmn:tFormalExpression">= is defined(odoo_task_id) and odoo_task_id != null</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="Flow_create_to_merge" sourceRef="Activity_create_main_task" targetRef="Gateway_odoo_merge" />"""


def user_task_xml(task_id, name, description, incoming_flows, outgoing_flow):
    """Generate a User Task with Odoo inputs (Rule #3)."""
    inc = "\n".join(f"      <bpmn:incoming>{f}</bpmn:incoming>" for f in incoming_flows)
    return f"""
    <bpmn:userTask id="{task_id}" name="{esc(name)}">
      <bpmn:extensionElements>
        <zeebe:userTask />
        <zeebe:ioMapping>
          <zeebe:input source="={Q}POST{Q}" target="method" />
          <zeebe:input source="={Q}{esc(WEBHOOK_URL)}{Q}" target="url" />
          <zeebe:input source="={{{Q}Content-Type{Q}:{Q}application/json{Q}}}" target="headers" />
          <zeebe:input source="={{name: {Q}{esc(name)}{Q}, description: {Q}{esc(description)}{Q}, _model: {Q}project.project{Q}, _id: 252, process_instance_key: process_instance_key}}" target="body" />
        </zeebe:ioMapping>
      </bpmn:extensionElements>
{inc}
      <bpmn:outgoing>{outgoing_flow}</bpmn:outgoing>
    </bpmn:userTask>"""


def boundary_events_xml(task_id, task_name):
    """Generate boundary timer events for a User Task (Rule #6)."""
    short_name = task_name[:40]
    return f"""
    <bpmn:boundaryEvent id="BE_rem_{task_id}" name="Нагадування" cancelActivity="false" attachedToRef="{task_id}">
      <bpmn:outgoing>Flow_rem_{task_id}</bpmn:outgoing>
      <bpmn:timerEventDefinition id="TD_rem_{task_id}">
        <bpmn:timeCycle xsi:type="bpmn:tFormalExpression">R/PT24H</bpmn:timeCycle>
      </bpmn:timerEventDefinition>
    </bpmn:boundaryEvent>
    <bpmn:serviceTask id="ST_rem_{task_id}" name="Надіслати нагадування">
      <bpmn:extensionElements>
        <zeebe:taskDefinition type="http-request-smart" />
      </bpmn:extensionElements>
      <bpmn:incoming>Flow_rem_{task_id}</bpmn:incoming>
    </bpmn:serviceTask>
    <bpmn:sequenceFlow id="Flow_rem_{task_id}" sourceRef="BE_rem_{task_id}" targetRef="ST_rem_{task_id}" />
    <bpmn:boundaryEvent id="BE_ded_{task_id}" name="Дедлайн" attachedToRef="{task_id}">
      <bpmn:outgoing>Flow_ded_{task_id}</bpmn:outgoing>
      <bpmn:timerEventDefinition id="TD_ded_{task_id}">
        <bpmn:timeDuration xsi:type="bpmn:tFormalExpression">P3D</bpmn:timeDuration>
      </bpmn:timerEventDefinition>
    </bpmn:boundaryEvent>
    <bpmn:userTask id="UT_esc_{task_id}" name="Ескалація: {esc(short_name)}">
      <bpmn:extensionElements><zeebe:userTask /></bpmn:extensionElements>
      <bpmn:incoming>Flow_ded_{task_id}</bpmn:incoming>
      <bpmn:outgoing>Flow_esc_end_{task_id}</bpmn:outgoing>
    </bpmn:userTask>
    <bpmn:endEvent id="EE_esc_{task_id}">
      <bpmn:incoming>Flow_esc_end_{task_id}</bpmn:incoming>
    </bpmn:endEvent>
    <bpmn:sequenceFlow id="Flow_ded_{task_id}" sourceRef="BE_ded_{task_id}" targetRef="UT_esc_{task_id}" />
    <bpmn:sequenceFlow id="Flow_esc_end_{task_id}" sourceRef="UT_esc_{task_id}" targetRef="EE_esc_{task_id}" />"""


def boundary_diagram(task_id, x, y):
    """Generate diagram shapes/edges for boundary events of a task at (x,y)."""
    return f"""
      <bpmndi:BPMNShape id="BE_rem_{task_id}_di" bpmnElement="BE_rem_{task_id}">
        <dc:Bounds x="{x+15}" y="{y+62}" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="ST_rem_{task_id}_di" bpmnElement="ST_rem_{task_id}">
        <dc:Bounds x="{x-10}" y="{y+120}" width="100" height="80" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="BE_ded_{task_id}_di" bpmnElement="BE_ded_{task_id}">
        <dc:Bounds x="{x+55}" y="{y+62}" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="UT_esc_{task_id}_di" bpmnElement="UT_esc_{task_id}">
        <dc:Bounds x="{x+30}" y="{y+230}" width="100" height="80" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="EE_esc_{task_id}_di" bpmnElement="EE_esc_{task_id}">
        <dc:Bounds x="{x+62}" y="{y+340}" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="Flow_rem_{task_id}_di" bpmnElement="Flow_rem_{task_id}">
        <di:waypoint x="{x+33}" y="{y+98}" />
        <di:waypoint x="{x+40}" y="{y+160}" />
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="Flow_ded_{task_id}_di" bpmnElement="Flow_ded_{task_id}">
        <di:waypoint x="{x+73}" y="{y+98}" />
        <di:waypoint x="{x+80}" y="{y+270}" />
      </bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="Flow_esc_end_{task_id}_di" bpmnElement="Flow_esc_end_{task_id}">
        <di:waypoint x="{x+80}" y="{y+310}" />
        <di:waypoint x="{x+80}" y="{y+340}" />
      </bpmndi:BPMNEdge>"""


XML_HEADER = """<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI" xmlns:dc="http://www.omg.org/spec/DD/20100524/DC" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" xmlns:modeler="http://camunda.org/schema/modeler/1.0" xmlns:di="http://www.omg.org/spec/DD/20100524/DI" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" id="Definitions_1" targetNamespace="http://bpmn.io/schema/bpmn" exporter="Camunda Web Modeler" exporterVersion="e787c33" modeler:executionPlatform="Camunda Cloud" modeler:executionPlatformVersion="8.8.0">"""


# ============================================================
# SUBPROCESS 1: Офіційний прийом на роботу
# ============================================================
def generate_oficiynyy_pryyom():
    tasks = [
        ("Activity_1cdkvuw", "Внести військові дані по працівнику",
         "Введіть військовий квиток та інші військові дані", ["Flow_merge_to_process"], "Flow_1ecpcgv"),
        ("Activity_0imr582", "Прописати організацію у фізичній особі",
         "Оберіть організацію та привяжіть до фізичної особи", ["Flow_1ecpcgv"], "Flow_0d6137m"),
        ("Activity_0hsv4gb", "Перевірити чи достатня кількість посад в штатному?",
         "Перевірте наявність вакантних посад у штатному розписі", ["Flow_08s7j0j", "Flow_0wrmn73"], "Flow_0h63vpp"),
        ("Activity_03c0jkn", "Додати посаду в штатний",
         "Додайте нову посаду до штатного розпису", ["Flow_0neqe0x"], "Flow_0wrmn73"),
        ("Activity_1w34e1k", "Змінити кількість одиниць в штатному",
         "Збільшіть кількість одиниць для обраної посади", ["Flow_0d2m1ig"], "Flow_034n49u"),
        ("Activity_14yzxcu", "Провести Наказ на прийом в БДУ",
         "Проведіть наказ на прийом в бухгалтерській системі", ["Flow_1sorzk1"], "Flow_0vmfydg"),
    ]

    process = f"""{XML_HEADER}
  <bpmn:process id="Process_0fx4kkx" name="Офіційний прийом на роботу" isExecutable="true">
    <bpmn:extensionElements><zeebe:versionTag value="2.0" /></bpmn:extensionElements>
    <bpmn:startEvent id="StartEvent_1">
      <bpmn:outgoing>Flow_to_odoo_xor</bpmn:outgoing>
    </bpmn:startEvent>
{odoo_start_pattern("Офіційний прийом на роботу")}
    <bpmn:sequenceFlow id="Flow_merge_to_process" sourceRef="Gateway_odoo_merge" targetRef="Activity_1cdkvuw" />"""

    # User tasks
    for t in tasks:
        process += user_task_xml(*t)

    # Service task (converted from plain task)
    process += """
    <bpmn:serviceTask id="Activity_0g8pv9q" name="Передача інформації та створення Прийому у БДУ">
      <bpmn:incoming>Flow_0b8rv0i</bpmn:incoming>
      <bpmn:incoming>Flow_034n49u</bpmn:incoming>
      <bpmn:outgoing>Flow_1sorzk1</bpmn:outgoing>
    </bpmn:serviceTask>"""

    # Gateways
    process += """
    <bpmn:exclusiveGateway id="Gateway_0dirkav" name="Є посада в БДУ?">
      <bpmn:incoming>Flow_0d6137m</bpmn:incoming>
      <bpmn:outgoing>Flow_08s7j0j</bpmn:outgoing>
      <bpmn:outgoing>Flow_0neqe0x</bpmn:outgoing>
    </bpmn:exclusiveGateway>
    <bpmn:exclusiveGateway id="Gateway_1csuvc1">
      <bpmn:incoming>Flow_0h63vpp</bpmn:incoming>
      <bpmn:outgoing>Flow_0d2m1ig</bpmn:outgoing>
      <bpmn:outgoing>Flow_0b8rv0i</bpmn:outgoing>
    </bpmn:exclusiveGateway>"""

    # End event
    process += """
    <bpmn:endEvent id="Event_14857la">
      <bpmn:incoming>Flow_0vmfydg</bpmn:incoming>
    </bpmn:endEvent>"""

    # Boundary events for all user tasks
    for tid, tname, _, _, _ in tasks:
        process += boundary_events_xml(tid, tname)

    # Sequence flows (Odoo pattern)
    process += odoo_start_flows()

    # Original flows
    process += """
    <bpmn:sequenceFlow id="Flow_1ecpcgv" sourceRef="Activity_1cdkvuw" targetRef="Activity_0imr582" />
    <bpmn:sequenceFlow id="Flow_0d6137m" sourceRef="Activity_0imr582" targetRef="Gateway_0dirkav" />
    <bpmn:sequenceFlow id="Flow_08s7j0j" sourceRef="Gateway_0dirkav" targetRef="Activity_0hsv4gb" />
    <bpmn:sequenceFlow id="Flow_0neqe0x" name="ні" sourceRef="Gateway_0dirkav" targetRef="Activity_03c0jkn" />
    <bpmn:sequenceFlow id="Flow_0wrmn73" sourceRef="Activity_03c0jkn" targetRef="Activity_0hsv4gb" />
    <bpmn:sequenceFlow id="Flow_0h63vpp" sourceRef="Activity_0hsv4gb" targetRef="Gateway_1csuvc1" />
    <bpmn:sequenceFlow id="Flow_0d2m1ig" name="так" sourceRef="Gateway_1csuvc1" targetRef="Activity_1w34e1k" />
    <bpmn:sequenceFlow id="Flow_0b8rv0i" sourceRef="Gateway_1csuvc1" targetRef="Activity_0g8pv9q" />
    <bpmn:sequenceFlow id="Flow_034n49u" sourceRef="Activity_1w34e1k" targetRef="Activity_0g8pv9q" />
    <bpmn:sequenceFlow id="Flow_1sorzk1" sourceRef="Activity_0g8pv9q" targetRef="Activity_14yzxcu" />
    <bpmn:sequenceFlow id="Flow_0vmfydg" sourceRef="Activity_14yzxcu" targetRef="Event_14857la" />"""

    process += """
  </bpmn:process>"""

    # Diagram
    task_positions = [
        ("Activity_1cdkvuw", 380, 140), ("Activity_0imr582", 530, 140),
        ("Activity_0hsv4gb", 780, 140), ("Activity_03c0jkn", 780, 280),
        ("Activity_1w34e1k", 1030, 140), ("Activity_14yzxcu", 1230, 140),
        ("Activity_0g8pv9q", 1080, 280),
    ]
    diagram = """
  <bpmndi:BPMNDiagram id="BPMNDiagram_1">
    <bpmndi:BPMNPlane id="BPMNPlane_1" bpmnElement="Process_0fx4kkx">
      <bpmndi:BPMNShape id="_BPMNShape_StartEvent_2" bpmnElement="StartEvent_1">
        <dc:Bounds x="52" y="162" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="Gateway_odoo_xor_di" bpmnElement="Gateway_odoo_xor" isMarkerVisible="true">
        <dc:Bounds x="125" y="155" width="50" height="50" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="Activity_create_main_task_di" bpmnElement="Activity_create_main_task">
        <dc:Bounds x="200" y="60" width="100" height="80" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="Gateway_odoo_merge_di" bpmnElement="Gateway_odoo_merge" isMarkerVisible="true">
        <dc:Bounds x="325" y="155" width="50" height="50" />
      </bpmndi:BPMNShape>"""

    for tid, x, y in task_positions:
        diagram += f"""
      <bpmndi:BPMNShape id="{tid}_di" bpmnElement="{tid}">
        <dc:Bounds x="{x}" y="{y}" width="100" height="80" />
      </bpmndi:BPMNShape>"""

    diagram += """
      <bpmndi:BPMNShape id="Gateway_0dirkav_di" bpmnElement="Gateway_0dirkav" isMarkerVisible="true">
        <dc:Bounds x="685" y="155" width="50" height="50" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="Gateway_1csuvc1_di" bpmnElement="Gateway_1csuvc1" isMarkerVisible="true">
        <dc:Bounds x="935" y="155" width="50" height="50" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="Event_14857la_di" bpmnElement="Event_14857la">
        <dc:Bounds x="1382" y="162" width="36" height="36" />
      </bpmndi:BPMNShape>"""

    # Boundary event diagram shapes
    for tid, x, y in task_positions[:6]:  # Only user tasks (not service task)
        diagram += boundary_diagram(tid, x, y)

    # Edges (simplified - just source→target with basic waypoints)
    edges = [
        ("Flow_to_odoo_xor", 88, 180, 125, 180),
        ("Flow_odoo_default", 150, 155, 250, 140),
        ("Flow_odoo_skip", 175, 180, 325, 180),
        ("Flow_create_to_merge", 300, 100, 350, 155),
        ("Flow_merge_to_process", 375, 180, 380, 180),
        ("Flow_1ecpcgv", 480, 180, 530, 180),
        ("Flow_0d6137m", 630, 180, 685, 180),
        ("Flow_08s7j0j", 735, 180, 780, 180),
        ("Flow_0neqe0x", 710, 205, 780, 320),
        ("Flow_0wrmn73", 830, 280, 830, 220),
        ("Flow_0h63vpp", 880, 180, 935, 180),
        ("Flow_0d2m1ig", 985, 180, 1030, 180),
        ("Flow_0b8rv0i", 960, 205, 1080, 320),
        ("Flow_034n49u", 1130, 180, 1130, 280),
        ("Flow_1sorzk1", 1180, 320, 1230, 180),
        ("Flow_0vmfydg", 1330, 180, 1382, 180),
    ]
    for fid, x1, y1, x2, y2 in edges:
        diagram += f"""
      <bpmndi:BPMNEdge id="{fid}_di" bpmnElement="{fid}">
        <di:waypoint x="{x1}" y="{y1}" />
        <di:waypoint x="{x2}" y="{y2}" />
      </bpmndi:BPMNEdge>"""

    diagram += """
    </bpmndi:BPMNPlane>
  </bpmndi:BPMNDiagram>
</bpmn:definitions>
"""

    return process + diagram


# ============================================================
# SUBPROCESS 2: Надання доступів для адміністратора
# ============================================================
def generate_dostupiv_admin():
    tasks = [
        ("Activity_0vsfezp", "Налаштувати сповіщення по прострочці задач резервування товарів",
         "Налаштуйте сповіщення для контролю задач резервування", ["Flow_1xolo2z"], "Flow_0srjj0y"),
        ("Activity_1o6meit", "Налаштувати відповідального працівника за прийом посилок на НП",
         "Призначте відповідального за прийом посилок", ["Flow_1600us0"], "Flow_1bpjh1p"),
        ("Activity_0pssvez", "Надати доступ до камер",
         "Надайте доступ до камер спостереження", ["Flow_1wq4r75"], "Flow_06rrhf9"),
    ]

    process = f"""{XML_HEADER}
  <bpmn:process id="Process_0et2gc9" name="Надання доступів для адміністратора" isExecutable="true">
    <bpmn:extensionElements><zeebe:versionTag value="2.0" /></bpmn:extensionElements>
    <bpmn:startEvent id="StartEvent_1">
      <bpmn:outgoing>Flow_to_odoo_xor</bpmn:outgoing>
    </bpmn:startEvent>
{odoo_start_pattern("Надання доступів для адміністратора")}
    <bpmn:sequenceFlow id="Flow_merge_to_process" sourceRef="Gateway_odoo_merge" targetRef="Gateway_045w5an" />"""

    # User tasks
    for t in tasks:
        process += user_task_xml(*t)

    # Service tasks (kept as-is from original)
    process += """
    <bpmn:serviceTask id="Activity_143h30f" name="Заповнити керівника у підрозділі">
      <bpmn:incoming>Flow_00oks2x</bpmn:incoming>
      <bpmn:outgoing>Flow_1c0l46o</bpmn:outgoing>
    </bpmn:serviceTask>
    <bpmn:serviceTask id="Activity_1mquwfd" name="Заповнити відповідального у складі магазину">
      <bpmn:incoming>Flow_02nicmt</bpmn:incoming>
      <bpmn:outgoing>Flow_1n7ojn2</bpmn:outgoing>
    </bpmn:serviceTask>"""

    # Gateways
    process += """
    <bpmn:parallelGateway id="Gateway_045w5an">
      <bpmn:incoming>Flow_merge_to_process</bpmn:incoming>
      <bpmn:outgoing>Flow_1xolo2z</bpmn:outgoing>
      <bpmn:outgoing>Flow_00oks2x</bpmn:outgoing>
      <bpmn:outgoing>Flow_02nicmt</bpmn:outgoing>
      <bpmn:outgoing>Flow_1600us0</bpmn:outgoing>
      <bpmn:outgoing>Flow_1wq4r75</bpmn:outgoing>
    </bpmn:parallelGateway>
    <bpmn:parallelGateway id="Gateway_13x6wj3">
      <bpmn:incoming>Flow_1n7ojn2</bpmn:incoming>
      <bpmn:incoming>Flow_1c0l46o</bpmn:incoming>
      <bpmn:incoming>Flow_0srjj0y</bpmn:incoming>
      <bpmn:incoming>Flow_1bpjh1p</bpmn:incoming>
      <bpmn:incoming>Flow_06rrhf9</bpmn:incoming>
      <bpmn:outgoing>Flow_0703nrw</bpmn:outgoing>
    </bpmn:parallelGateway>"""

    # End event
    process += """
    <bpmn:endEvent id="Event_001ose2">
      <bpmn:incoming>Flow_0703nrw</bpmn:incoming>
    </bpmn:endEvent>"""

    # Boundary events
    for tid, tname, _, _, _ in tasks:
        process += boundary_events_xml(tid, tname)

    # Flows
    process += odoo_start_flows()
    process += """
    <bpmn:sequenceFlow id="Flow_1xolo2z" sourceRef="Gateway_045w5an" targetRef="Activity_0vsfezp" />
    <bpmn:sequenceFlow id="Flow_00oks2x" sourceRef="Gateway_045w5an" targetRef="Activity_143h30f" />
    <bpmn:sequenceFlow id="Flow_02nicmt" sourceRef="Gateway_045w5an" targetRef="Activity_1mquwfd" />
    <bpmn:sequenceFlow id="Flow_1600us0" sourceRef="Gateway_045w5an" targetRef="Activity_1o6meit" />
    <bpmn:sequenceFlow id="Flow_1wq4r75" sourceRef="Gateway_045w5an" targetRef="Activity_0pssvez" />
    <bpmn:sequenceFlow id="Flow_1n7ojn2" sourceRef="Activity_1mquwfd" targetRef="Gateway_13x6wj3" />
    <bpmn:sequenceFlow id="Flow_1c0l46o" sourceRef="Activity_143h30f" targetRef="Gateway_13x6wj3" />
    <bpmn:sequenceFlow id="Flow_0srjj0y" sourceRef="Activity_0vsfezp" targetRef="Gateway_13x6wj3" />
    <bpmn:sequenceFlow id="Flow_1bpjh1p" sourceRef="Activity_1o6meit" targetRef="Gateway_13x6wj3" />
    <bpmn:sequenceFlow id="Flow_06rrhf9" sourceRef="Activity_0pssvez" targetRef="Gateway_13x6wj3" />
    <bpmn:sequenceFlow id="Flow_0703nrw" sourceRef="Gateway_13x6wj3" targetRef="Event_001ose2" />"""

    process += """
  </bpmn:process>"""

    # Diagram
    diagram = """
  <bpmndi:BPMNDiagram id="BPMNDiagram_1">
    <bpmndi:BPMNPlane id="BPMNPlane_1" bpmnElement="Process_0et2gc9">
      <bpmndi:BPMNShape id="_BPMNShape_StartEvent_2" bpmnElement="StartEvent_1">
        <dc:Bounds x="52" y="252" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="Gateway_odoo_xor_di" bpmnElement="Gateway_odoo_xor" isMarkerVisible="true">
        <dc:Bounds x="125" y="245" width="50" height="50" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="Activity_create_main_task_di" bpmnElement="Activity_create_main_task">
        <dc:Bounds x="200" y="140" width="100" height="80" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="Gateway_odoo_merge_di" bpmnElement="Gateway_odoo_merge" isMarkerVisible="true">
        <dc:Bounds x="335" y="245" width="50" height="50" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="Gateway_045w5an_di" bpmnElement="Gateway_045w5an">
        <dc:Bounds x="425" y="245" width="50" height="50" />
      </bpmndi:BPMNShape>"""

    task_pos = [
        ("Activity_1mquwfd", 540, 30), ("Activity_143h30f", 540, 130),
        ("Activity_0vsfezp", 540, 230), ("Activity_1o6meit", 540, 330),
        ("Activity_0pssvez", 540, 430),
    ]
    for tid, x, y in task_pos:
        diagram += f"""
      <bpmndi:BPMNShape id="{tid}_di" bpmnElement="{tid}">
        <dc:Bounds x="{x}" y="{y}" width="100" height="80" />
      </bpmndi:BPMNShape>"""

    diagram += """
      <bpmndi:BPMNShape id="Gateway_13x6wj3_di" bpmnElement="Gateway_13x6wj3">
        <dc:Bounds x="695" y="245" width="50" height="50" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="Event_001ose2_di" bpmnElement="Event_001ose2">
        <dc:Bounds x="792" y="252" width="36" height="36" />
      </bpmndi:BPMNShape>"""

    # Boundary diagrams for user tasks only
    ut_pos = [("Activity_0vsfezp", 540, 230), ("Activity_1o6meit", 540, 330), ("Activity_0pssvez", 540, 430)]
    for tid, x, y in ut_pos:
        diagram += boundary_diagram(tid, x, y)

    # Edges
    edges = [
        ("Flow_to_odoo_xor", 88, 270, 125, 270), ("Flow_odoo_default", 150, 245, 250, 220),
        ("Flow_odoo_skip", 175, 270, 335, 270), ("Flow_create_to_merge", 300, 180, 360, 245),
        ("Flow_merge_to_process", 385, 270, 425, 270),
        ("Flow_1xolo2z", 475, 270, 540, 270), ("Flow_00oks2x", 450, 245, 540, 170),
        ("Flow_02nicmt", 450, 245, 540, 70), ("Flow_1600us0", 450, 295, 540, 370),
        ("Flow_1wq4r75", 450, 295, 540, 470),
        ("Flow_1n7ojn2", 640, 70, 720, 245), ("Flow_1c0l46o", 640, 170, 720, 245),
        ("Flow_0srjj0y", 640, 270, 695, 270), ("Flow_1bpjh1p", 640, 370, 720, 295),
        ("Flow_06rrhf9", 640, 470, 720, 295), ("Flow_0703nrw", 745, 270, 792, 270),
    ]
    for fid, x1, y1, x2, y2 in edges:
        diagram += f"""
      <bpmndi:BPMNEdge id="{fid}_di" bpmnElement="{fid}">
        <di:waypoint x="{x1}" y="{y1}" />
        <di:waypoint x="{x2}" y="{y2}" />
      </bpmndi:BPMNEdge>"""

    diagram += """
    </bpmndi:BPMNPlane>
  </bpmndi:BPMNDiagram>
</bpmn:definitions>
"""
    return process + diagram


# ============================================================
# SUBPROCESS 3: Надання доступів та ОЗ
# ============================================================
def generate_dostupiv_oz():
    tasks = [
        ("Activity_1qzyyoj", "Створити користувача в Passbolt",
         "Створіть обліковий запис в Passbolt", ["Flow_0lah2nk"], "Flow_1lafgyv"),
        ("Activity_0qai6qv", "Надати Адмін доступи на сайті",
         "Надайте адміністративні доступи", ["Flow_1ui5p6c"], "Flow_0j3rzr9"),
        ("Activity_1ux9sob", "Видати співробітнику ОЗ",
         "Видайте основні засоби працівнику", ["Flow_1d3ct82"], "Flow_0oqosxj"),
        ("Activity_0hlwyxn", "Надати доступ до електронних ключів",
         "Надайте доступ до електронних ключів", ["Flow_0hfc3y4"], "Flow_12h7vmu"),
        ("Activity_0cqoubb", "Допомогти налаштувати Passbolt",
         "Допоможіть працівнику налаштувати Passbolt", ["Flow_1lafgyv"], "Flow_0yk6gnc"),
        ("Activity_0bzvot4", "Доступи створені, ОЗ видано",
         "Підтвердіть створення доступів та видачу ОЗ", ["Flow_1y5v2z1"], "Flow_1r681yk"),
    ]

    process = f"""{XML_HEADER}
  <bpmn:process id="Process_0h8z3ny" name="Надання доступів та ОЗ" isExecutable="true">
    <bpmn:extensionElements><zeebe:versionTag value="2.0" /></bpmn:extensionElements>
    <bpmn:startEvent id="StartEvent_1">
      <bpmn:outgoing>Flow_to_odoo_xor</bpmn:outgoing>
    </bpmn:startEvent>
{odoo_start_pattern("Надання доступів та ОЗ")}
    <bpmn:sequenceFlow id="Flow_merge_to_process" sourceRef="Gateway_odoo_merge" targetRef="Gateway_0tkdon6" />"""

    # User tasks
    for t in tasks:
        process += user_task_xml(*t)

    # Service task
    process += """
    <bpmn:serviceTask id="Activity_0j2pm4k" name="Створити склад в ЄРП">
      <bpmn:incoming>Flow_07sxp4q</bpmn:incoming>
      <bpmn:outgoing>Flow_0ne7wti</bpmn:outgoing>
    </bpmn:serviceTask>"""

    # Gateways
    process += """
    <bpmn:parallelGateway id="Gateway_0tkdon6">
      <bpmn:incoming>Flow_merge_to_process</bpmn:incoming>
      <bpmn:outgoing>Flow_07sxp4q</bpmn:outgoing>
      <bpmn:outgoing>Flow_0lah2nk</bpmn:outgoing>
      <bpmn:outgoing>Flow_1ui5p6c</bpmn:outgoing>
      <bpmn:outgoing>Flow_1d3ct82</bpmn:outgoing>
      <bpmn:outgoing>Flow_0hfc3y4</bpmn:outgoing>
    </bpmn:parallelGateway>
    <bpmn:inclusiveGateway id="Gateway_0m8i0n3">
      <bpmn:incoming>Flow_0ne7wti</bpmn:incoming>
      <bpmn:incoming>Flow_0yk6gnc</bpmn:incoming>
      <bpmn:incoming>Flow_0j3rzr9</bpmn:incoming>
      <bpmn:incoming>Flow_0oqosxj</bpmn:incoming>
      <bpmn:incoming>Flow_12h7vmu</bpmn:incoming>
      <bpmn:outgoing>Flow_1y5v2z1</bpmn:outgoing>
    </bpmn:inclusiveGateway>"""

    # End event
    process += """
    <bpmn:endEvent id="Event_19el0tz">
      <bpmn:incoming>Flow_1r681yk</bpmn:incoming>
    </bpmn:endEvent>"""

    # Text annotation
    process += """
    <bpmn:textAnnotation id="TextAnnotation_0czmbvj">
      <bpmn:text>Назва задачі в різна підрозділів</bpmn:text>
    </bpmn:textAnnotation>
    <bpmn:association id="Association_0khm6de" associationDirection="None" sourceRef="Activity_0bzvot4" targetRef="TextAnnotation_0czmbvj" />"""

    # Boundary events
    for tid, tname, _, _, _ in tasks:
        process += boundary_events_xml(tid, tname)

    # Flows
    process += odoo_start_flows()
    process += """
    <bpmn:sequenceFlow id="Flow_07sxp4q" sourceRef="Gateway_0tkdon6" targetRef="Activity_0j2pm4k" />
    <bpmn:sequenceFlow id="Flow_0lah2nk" sourceRef="Gateway_0tkdon6" targetRef="Activity_1qzyyoj" />
    <bpmn:sequenceFlow id="Flow_1ui5p6c" sourceRef="Gateway_0tkdon6" targetRef="Activity_0qai6qv" />
    <bpmn:sequenceFlow id="Flow_1d3ct82" sourceRef="Gateway_0tkdon6" targetRef="Activity_1ux9sob" />
    <bpmn:sequenceFlow id="Flow_0hfc3y4" sourceRef="Gateway_0tkdon6" targetRef="Activity_0hlwyxn" />
    <bpmn:sequenceFlow id="Flow_1lafgyv" sourceRef="Activity_1qzyyoj" targetRef="Activity_0cqoubb" />
    <bpmn:sequenceFlow id="Flow_0ne7wti" sourceRef="Activity_0j2pm4k" targetRef="Gateway_0m8i0n3" />
    <bpmn:sequenceFlow id="Flow_0yk6gnc" sourceRef="Activity_0cqoubb" targetRef="Gateway_0m8i0n3" />
    <bpmn:sequenceFlow id="Flow_0j3rzr9" sourceRef="Activity_0qai6qv" targetRef="Gateway_0m8i0n3" />
    <bpmn:sequenceFlow id="Flow_0oqosxj" sourceRef="Activity_1ux9sob" targetRef="Gateway_0m8i0n3" />
    <bpmn:sequenceFlow id="Flow_12h7vmu" sourceRef="Activity_0hlwyxn" targetRef="Gateway_0m8i0n3" />
    <bpmn:sequenceFlow id="Flow_1y5v2z1" sourceRef="Gateway_0m8i0n3" targetRef="Activity_0bzvot4" />
    <bpmn:sequenceFlow id="Flow_1r681yk" sourceRef="Activity_0bzvot4" targetRef="Event_19el0tz" />"""

    process += """
  </bpmn:process>"""

    # Diagram
    diagram = """
  <bpmndi:BPMNDiagram id="BPMNDiagram_1">
    <bpmndi:BPMNPlane id="BPMNPlane_1" bpmnElement="Process_0h8z3ny">
      <bpmndi:BPMNShape id="_BPMNShape_StartEvent_2" bpmnElement="StartEvent_1">
        <dc:Bounds x="52" y="252" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="Gateway_odoo_xor_di" bpmnElement="Gateway_odoo_xor" isMarkerVisible="true">
        <dc:Bounds x="125" y="245" width="50" height="50" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="Activity_create_main_task_di" bpmnElement="Activity_create_main_task">
        <dc:Bounds x="200" y="140" width="100" height="80" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="Gateway_odoo_merge_di" bpmnElement="Gateway_odoo_merge" isMarkerVisible="true">
        <dc:Bounds x="335" y="245" width="50" height="50" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="Gateway_0tkdon6_di" bpmnElement="Gateway_0tkdon6">
        <dc:Bounds x="425" y="245" width="50" height="50" />
      </bpmndi:BPMNShape>"""

    task_pos = [
        ("Activity_0j2pm4k", 540, 80), ("Activity_1qzyyoj", 540, 230),
        ("Activity_0cqoubb", 680, 230), ("Activity_0qai6qv", 540, 370),
        ("Activity_1ux9sob", 540, 500), ("Activity_0hlwyxn", 540, 620),
        ("Activity_0bzvot4", 930, 230),
    ]
    for tid, x, y in task_pos:
        diagram += f"""
      <bpmndi:BPMNShape id="{tid}_di" bpmnElement="{tid}">
        <dc:Bounds x="{x}" y="{y}" width="100" height="80" />
      </bpmndi:BPMNShape>"""

    diagram += """
      <bpmndi:BPMNShape id="Gateway_0m8i0n3_di" bpmnElement="Gateway_0m8i0n3">
        <dc:Bounds x="835" y="245" width="50" height="50" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="Event_19el0tz_di" bpmnElement="Event_19el0tz">
        <dc:Bounds x="1092" y="252" width="36" height="36" />
      </bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="TextAnnotation_0czmbvj_di" bpmnElement="TextAnnotation_0czmbvj">
        <dc:Bounds x="1030" y="150" width="100" height="55" />
      </bpmndi:BPMNShape>"""

    # Boundary diagrams for user tasks
    ut_pos = [
        ("Activity_1qzyyoj", 540, 230), ("Activity_0qai6qv", 540, 370),
        ("Activity_1ux9sob", 540, 500), ("Activity_0hlwyxn", 540, 620),
        ("Activity_0cqoubb", 680, 230), ("Activity_0bzvot4", 930, 230),
    ]
    for tid, x, y in ut_pos:
        diagram += boundary_diagram(tid, x, y)

    # Edges
    edges = [
        ("Flow_to_odoo_xor", 88, 270, 125, 270), ("Flow_odoo_default", 150, 245, 250, 220),
        ("Flow_odoo_skip", 175, 270, 335, 270), ("Flow_create_to_merge", 300, 180, 360, 245),
        ("Flow_merge_to_process", 385, 270, 425, 270),
        ("Flow_07sxp4q", 450, 245, 540, 120), ("Flow_0lah2nk", 475, 270, 540, 270),
        ("Flow_1ui5p6c", 450, 295, 540, 410), ("Flow_1d3ct82", 450, 295, 540, 540),
        ("Flow_0hfc3y4", 450, 295, 540, 660), ("Flow_1lafgyv", 640, 270, 680, 270),
        ("Flow_0ne7wti", 640, 120, 860, 245), ("Flow_0yk6gnc", 780, 270, 835, 270),
        ("Flow_0j3rzr9", 640, 410, 860, 295), ("Flow_0oqosxj", 640, 540, 860, 295),
        ("Flow_12h7vmu", 640, 660, 860, 295), ("Flow_1y5v2z1", 885, 270, 930, 270),
        ("Flow_1r681yk", 1030, 270, 1092, 270),
    ]
    for fid, x1, y1, x2, y2 in edges:
        diagram += f"""
      <bpmndi:BPMNEdge id="{fid}_di" bpmnElement="{fid}">
        <di:waypoint x="{x1}" y="{y1}" />
        <di:waypoint x="{x2}" y="{y2}" />
      </bpmndi:BPMNEdge>"""

    diagram += """
      <bpmndi:BPMNEdge id="Association_0khm6de_di" bpmnElement="Association_0khm6de">
        <di:waypoint x="1018" y="230" />
        <di:waypoint x="1042" y="205" />
      </bpmndi:BPMNEdge>
    </bpmndi:BPMNPlane>
  </bpmndi:BPMNDiagram>
</bpmn:definitions>
"""
    return process + diagram


# ============================================================
# MAIN: Generate all 3 files
# ============================================================
if __name__ == "__main__":
    base = "/opt/camunda/docker-compose-8.8"

    files = [
        (f"{base}/oficiynyy-pryyom-v2.bpmn", generate_oficiynyy_pryyom),
        (f"{base}/nadannya-dostupiv-admin-v2.bpmn", generate_dostupiv_admin),
        (f"{base}/nadannya-dostupiv-oz-v2.bpmn", generate_dostupiv_oz),
    ]

    for path, gen_func in files:
        content = gen_func()
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        lines = content.count("\n") + 1
        print(f"Created {path} ({lines} lines)")

    print("\nAll 3 subprocess files generated successfully!")
