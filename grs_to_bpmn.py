#!/usr/bin/env python3
"""
GRS to BPMN Converter for Camunda 8.8
======================================
Парсить файли формату GRAPES (.grs) та конвертує у BPMN XML
з усіма правилами проекту (CLAUDE.md).

Використання:
    python3 grs_to_bpmn.py input.grs output.bpmn --name "Назва процесу"
"""

import re
import sys
import argparse
from dataclasses import dataclass, field
from typing import Optional
from collections import OrderedDict

# ============================================================
# MODULE 1: TOKENIZER
# ============================================================

class Token:
    __slots__ = ('type', 'value')
    def __init__(self, type_, value):
        self.type = type_
        self.value = value
    def __repr__(self):
        return f'Token({self.type}, {self.value!r})'

def tokenize(text: str) -> list:
    """Tokenize .grs file content into a list of tokens."""
    tokens = []
    i = 0
    n = len(text)
    while i < n:
        c = text[i]
        # Skip whitespace
        if c in ' \t\n\r':
            i += 1
            continue
        # Braces
        if c == '{':
            tokens.append(Token('LBRACE', '{'))
            i += 1
        elif c == '}':
            tokens.append(Token('RBRACE', '}'))
            i += 1
        elif c == ',':
            tokens.append(Token('COMMA', ','))
            i += 1
        # Quoted string
        elif c == '"':
            j = i + 1
            while j < n and text[j] != '"':
                if text[j] == '\\':
                    j += 1  # skip escaped char
                j += 1
            value = text[i+1:j]
            tokens.append(Token('STRING', value))
            i = j + 1
        # Number (possibly negative)
        elif c == '-' or c.isdigit():
            j = i + 1 if c == '-' else i
            while j < n and text[j].isdigit():
                j += 1
            tokens.append(Token('NUMBER', int(text[i:j])))
            i = j
        # UUID (hex-dash pattern) or identifier
        elif c.isalpha() or c in '_':
            j = i
            while j < n and (text[j].isalnum() or text[j] in '-_'):
                j += 1
            value = text[i:j]
            tokens.append(Token('IDENT', value))
            i = j
        else:
            i += 1  # skip unknown chars
    return tokens


# ============================================================
# MODULE 2: RECURSIVE PARSER
# ============================================================

class Parser:
    """Parse token stream into nested Python lists."""

    def __init__(self, tokens):
        self.tokens = tokens
        self.pos = 0

    def peek(self):
        if self.pos < len(self.tokens):
            return self.tokens[self.pos]
        return None

    def consume(self):
        t = self.tokens[self.pos]
        self.pos += 1
        return t

    def parse(self):
        """Parse the entire token stream as a single value."""
        return self._parse_value()

    def _parse_value(self):
        t = self.peek()
        if t is None:
            return None
        if t.type == 'LBRACE':
            return self._parse_list()
        elif t.type in ('NUMBER', 'STRING', 'IDENT'):
            self.consume()
            return t.value
        else:
            self.consume()
            return None

    def _parse_list(self):
        """Parse {a, b, c, ...} into a Python list."""
        self.consume()  # eat LBRACE
        items = []
        while True:
            t = self.peek()
            if t is None or t.type == 'RBRACE':
                break
            items.append(self._parse_value())
            # Skip comma
            t = self.peek()
            if t and t.type == 'COMMA':
                self.consume()
        # eat RBRACE
        t = self.peek()
        if t and t.type == 'RBRACE':
            self.consume()
        return items


# ============================================================
# MODULE 3: EXTRACTOR
# ============================================================

GRS_TYPES = {
    1: 'line',
    2: 'start',
    3: 'end',
    4: 'gateway',
    7: 'parallel',
    9: 'task',
}

@dataclass
class GrsElement:
    id: int
    type: str          # 'start', 'end', 'gateway', 'parallel', 'task'
    label: str = ''
    internal_name: str = ''
    uuid: str = ''
    index: int = 0

@dataclass
class GrsConnection:
    from_id: int
    to_id: int
    branch_index: int = 0
    label: str = ''
    line_id: int = 0

@dataclass
class ProcessGraph:
    elements: dict = field(default_factory=dict)       # id -> GrsElement
    connections: list = field(default_factory=list)     # list of GrsConnection

    def get_outgoing(self, elem_id: int) -> list:
        return [c for c in self.connections if c.from_id == elem_id]

    def get_incoming(self, elem_id: int) -> list:
        return [c for c in self.connections if c.to_id == elem_id]

    def get_start(self) -> Optional[GrsElement]:
        for e in self.elements.values():
            if e.type == 'start':
                return e
        return None

    def get_end(self) -> Optional[GrsElement]:
        for e in self.elements.values():
            if e.type == 'end':
                return e
        return None

    def topo_order(self) -> list:
        """Topological sort of elements following connections."""
        visited = set()
        order = []

        def dfs(eid):
            if eid in visited or eid not in self.elements:
                return
            visited.add(eid)
            for conn in self.get_outgoing(eid):
                dfs(conn.to_id)
            order.append(eid)

        start = self.get_start()
        if start:
            dfs(start.id)
        # Also visit any unvisited elements
        for eid in self.elements:
            if eid not in visited:
                dfs(eid)

        order.reverse()
        return order


def extract_label(props) -> str:
    """Extract display label from properties like [1,1,['#','label']]."""
    if isinstance(props, list) and len(props) >= 3:
        inner = props[2]
        if isinstance(inner, list) and len(inner) >= 2 and inner[0] == '#':
            return inner[1]
    return ''


def extract_element_def(block) -> Optional[tuple]:
    """Extract (id, props, internal_name, index) from element definition block.
    Element def: [4, <id>, <properties>, '<name>', <index>]
    """
    if not isinstance(block, list) or len(block) < 5:
        return None
    if block[0] != 4:
        return None
    eid = block[1]
    if not isinstance(eid, int) or eid <= 0:
        return None  # Skip visual data blocks with id=0
    props = block[2]
    name = block[3] if isinstance(block[3], str) else ''
    idx = block[4] if len(block) > 4 and isinstance(block[4], int) else 0
    return (eid, props, name, idx)


def extract_process(parsed) -> ProcessGraph:
    """Walk the parsed tree and extract all elements and connections."""
    graph = ProcessGraph()

    def _find_elem_def_recursive(data, max_depth=3):
        """Recursively search for an element definition [4, id, props, name, idx]."""
        if not isinstance(data, list) or max_depth <= 0:
            return None, ''
        # Check if this IS an elem_def
        ed = extract_element_def(data)
        if ed:
            return ed, ''
        # Search children
        found_ed = None
        found_uuid = ''
        for item in data:
            if isinstance(item, list) and found_ed is None:
                ed = extract_element_def(item)
                if ed:
                    found_ed = ed
                else:
                    sub_ed, sub_uuid = _find_elem_def_recursive(item, max_depth - 1)
                    if sub_ed:
                        found_ed = sub_ed
                    if sub_uuid:
                        found_uuid = sub_uuid
            elif isinstance(item, str) and '-' in item and len(item) > 30:
                found_uuid = item
        return found_ed, found_uuid

    def walk(data, depth=0):
        """Recursively walk the nested structure to find elements and lines."""
        if not isinstance(data, list) or depth > 20:
            return

        i = 0
        while i < len(data):
            item = data[i]

            # Check if this is a type code followed by element block
            if isinstance(item, int) and item in GRS_TYPES:
                type_name = GRS_TYPES[item]
                # Next item should be the element block
                if i + 1 < len(data) and isinstance(data[i+1], list):
                    elem_block = data[i+1]
                    _process_element_block(graph, elem_block, type_name)
                    # Also recurse into the block to find nested elements
                    walk(elem_block, depth + 1)
                    i += 2
                    continue

            # Check if this item is itself a list containing elements
            if isinstance(item, list):
                # Check for line data: look for pattern where subtype=3
                _check_line_data(graph, item)
                # Recurse into sublists
                walk(item, depth + 1)

            i += 1

    def _process_element_block(graph, block, type_name):
        """Process an element block to extract element info."""
        if not isinstance(block, list) or len(block) < 1:
            return

        # For line type, also extract connection data
        if type_name == 'line':
            _check_line_data(graph, block)

        # Recursively find element definition and UUID
        elem_def_block, uuid = _find_elem_def_recursive(block)

        if elem_def_block:
            eid, props, name, idx = elem_def_block
            label = extract_label(props)
            elem = GrsElement(
                id=eid,
                type=type_name,
                label=label,
                internal_name=name,
                uuid=uuid,
                index=idx,
            )
            graph.elements[eid] = elem

    def _check_line_data(graph, block):
        """Check if a block contains line connection data.
        Line pattern: [{def}, 3, from_id, branch, to_id, flags, {visual}]
        """
        if not isinstance(block, list) or len(block) < 6:
            return

        # Look for the pattern: first item is element def, then 3 (line subtype)
        elem_def = None
        line_start = -1

        for i, item in enumerate(block):
            if isinstance(item, list):
                ed = extract_element_def(item)
                if ed:
                    elem_def = ed
            elif item == 3 and elem_def is not None and i > 0:
                # Check if next items are numbers (from_id, branch, to_id)
                if (i + 3 < len(block) and
                    isinstance(block[i+1], int) and
                    isinstance(block[i+2], int) and
                    isinstance(block[i+3], int)):
                    line_start = i
                    break

        if line_start >= 0 and elem_def:
            line_id, props, name, idx = elem_def
            from_id = block[line_start + 1]
            branch_idx = block[line_start + 2]
            to_id = block[line_start + 3]
            label = extract_label(props)

            conn = GrsConnection(
                from_id=from_id,
                to_id=to_id,
                branch_index=branch_idx,
                label=label,
                line_id=line_id,
            )
            # Avoid duplicate connections
            for existing in graph.connections:
                if existing.from_id == from_id and existing.to_id == to_id and existing.line_id == line_id:
                    return
            graph.connections.append(conn)

    # Start walking from root
    walk(parsed)
    return graph


# ============================================================
# MODULE 4: BPMN GENERATOR
# ============================================================

def slugify(text: str) -> str:
    """Convert Ukrainian text to ASCII slug for IDs."""
    # Transliteration map
    ua_map = {
        'а':'a','б':'b','в':'v','г':'h','ґ':'g','д':'d','е':'e','є':'ye',
        'ж':'zh','з':'z','и':'y','і':'i','ї':'yi','й':'y','к':'k','л':'l',
        'м':'m','н':'n','о':'o','п':'p','р':'r','с':'s','т':'t','у':'u',
        'ф':'f','х':'kh','ц':'ts','ч':'ch','ш':'sh','щ':'shch','ь':'',
        'ю':'yu','я':'ya','ё':'yo','э':'e','ы':'y','ъ':'',
    }
    result = []
    for c in text.lower():
        if c in ua_map:
            result.append(ua_map[c])
        elif c.isascii() and c.isalnum():
            result.append(c)
        elif c in ' _-':
            result.append('_')
    slug = '_'.join(filter(None, ''.join(result).split('_')))
    return slug[:50]  # limit length


def generate_bpmn(graph: ProcessGraph, process_name: str,
                   lane_name: str = "Бухгалтер") -> str:
    """Generate complete BPMN XML from ProcessGraph applying CLAUDE.md rules."""

    # Build element order for layout
    topo = graph.topo_order()

    # Collect tasks (for boundary events generation)
    tasks = {eid: e for eid, e in graph.elements.items() if e.type == 'task'}
    gateways = {eid: e for eid, e in graph.elements.items() if e.type == 'gateway'}
    parallels = {eid: e for eid, e in graph.elements.items() if e.type == 'parallel'}

    # Generate BPMN IDs from labels
    bpmn_ids = {}  # grs_id -> bpmn_id
    for eid, elem in graph.elements.items():
        slug = slugify(elem.label or elem.internal_name)
        if elem.type == 'start':
            bpmn_ids[eid] = 'StartEvent_1'
        elif elem.type == 'end':
            bpmn_ids[eid] = 'End_final'
        elif elem.type == 'task':
            bpmn_ids[eid] = f'UT_{slug}'
        elif elem.type == 'gateway':
            bpmn_ids[eid] = f'GW_{slug}'
        elif elem.type == 'parallel':
            bpmn_ids[eid] = f'GW_parallel_{slug}'
        else:
            bpmn_ids[eid] = f'Elem_{slug}'

    # Ensure unique IDs
    seen = set()
    for eid in list(bpmn_ids.keys()):
        bid = bpmn_ids[eid]
        if bid in seen:
            bid = f'{bid}_{eid}'
            bpmn_ids[eid] = bid
        seen.add(bid)

    # Determine which tasks are in which lane based on context
    # For now: all tasks go to Lane_responsible, gateways to Lane_system
    system_elements = ['StartEvent_1', 'End_final', 'End_not_approved',
                       'ST_create_main', 'GW_odoo_check', 'GW_odoo_merge']

    # Build flow IDs
    flow_ids = {}  # (from_grs_id, to_grs_id) -> flow_bpmn_id
    for conn in graph.connections:
        slug = slugify(conn.label) if conn.label else f'{conn.from_id}_to_{conn.to_id}'
        fid = f'Flow_{slug}_{conn.line_id}'
        flow_ids[(conn.from_id, conn.to_id)] = fid

    # ========== BUILD XML ==========
    lines = []

    def L(indent, text):
        lines.append('  ' * indent + text)

    # Header
    L(0, '<?xml version="1.0" encoding="UTF-8"?>')
    L(0, '<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" '
         'xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI" '
         'xmlns:dc="http://www.omg.org/spec/DD/20100524/DC" '
         'xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" '
         'xmlns:modeler="http://camunda.org/schema/modeler/1.0" '
         'xmlns:di="http://www.omg.org/spec/DD/20100524/DI" '
         'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
         'id="Definitions_1" targetNamespace="http://bpmn.io/schema/bpmn" '
         'exporter="grs_to_bpmn.py" exporterVersion="1.0" '
         'modeler:executionPlatform="Camunda Cloud" '
         'modeler:executionPlatformVersion="8.8.0">')

    process_id = f'Process_{slugify(process_name)}'

    # Collaboration
    L(1, '<bpmn:collaboration id="Collaboration_1">')
    L(2, f'<bpmn:participant id="Participant_1" name="{_xml_escape(process_name)}" processRef="{process_id}" />')
    L(1, '</bpmn:collaboration>')

    # Process
    L(1, f'<bpmn:process id="{process_id}" name="{_xml_escape(process_name)}" isExecutable="true">')
    L(2, '<bpmn:extensionElements>')
    L(3, '<zeebe:versionTag value="1.0" />')
    L(2, '</bpmn:extensionElements>')

    # --- LANES ---
    # Collect all element IDs per lane
    lane_system_refs = []
    lane_responsible_refs = []
    lane_manager_refs = []

    # Odoo check elements (added by generator)
    added_elements = ['StartEvent_1', 'GW_odoo_check', 'ST_create_main', 'GW_odoo_merge']
    lane_system_refs.extend(added_elements)

    # Assign original elements to lanes
    for eid, elem in graph.elements.items():
        bid = bpmn_ids[eid]
        if elem.type in ('start', 'end'):
            if bid not in lane_system_refs:
                lane_system_refs.append(bid)
        elif elem.type in ('gateway', 'parallel'):
            lane_system_refs.append(bid)
        elif elem.type == 'task':
            lane_responsible_refs.append(bid)

    # Add boundary event elements
    for eid, elem in tasks.items():
        bid = bpmn_ids[eid]
        suffix = bid.replace('UT_', '')
        # Reminder ST + End go to system lane
        lane_system_refs.append(f'ST_rem_{suffix}')
        lane_system_refs.append(f'End_rem_{suffix}')
        # Boundary events referenced from parent task
        # Escalation UT + End go to manager lane
        lane_manager_refs.append(f'UT_esc_{suffix}')
        lane_manager_refs.append(f'End_esc_{suffix}')

    L(2, '<bpmn:laneSet id="LaneSet_1">')

    # Lane: System
    L(3, '<bpmn:lane id="Lane_system" name="Система (автоматично)">')
    for ref in lane_system_refs:
        L(4, f'<bpmn:flowNodeRef>{ref}</bpmn:flowNodeRef>')
    L(3, '</bpmn:lane>')

    # Lane: Responsible
    L(3, f'<bpmn:lane id="Lane_responsible" name="{_xml_escape(lane_name)}">')
    for ref in lane_responsible_refs:
        L(4, f'<bpmn:flowNodeRef>{ref}</bpmn:flowNodeRef>')
    L(3, '</bpmn:lane>')

    # Lane: Manager
    L(3, '<bpmn:lane id="Lane_manager" name="Керівник (ескалація)">')
    for ref in lane_manager_refs:
        L(4, f'<bpmn:flowNodeRef>{ref}</bpmn:flowNodeRef>')
    L(3, '</bpmn:lane>')

    L(2, '</bpmn:laneSet>')

    # --- START EVENT ---
    L(2, '<bpmn:startEvent id="StartEvent_1">')
    L(3, '<bpmn:outgoing>Flow_start_to_check</bpmn:outgoing>')
    L(2, '</bpmn:startEvent>')

    # --- GW_odoo_check ---
    L(2, '<bpmn:exclusiveGateway id="GW_odoo_check" name="Задача в Odoo існує?" default="Flow_odoo_default">')
    L(3, '<bpmn:incoming>Flow_start_to_check</bpmn:incoming>')
    L(3, '<bpmn:outgoing>Flow_odoo_skip</bpmn:outgoing>')
    L(3, '<bpmn:outgoing>Flow_odoo_default</bpmn:outgoing>')
    L(2, '</bpmn:exclusiveGateway>')

    # --- ST_create_main ---
    L(2, '<bpmn:serviceTask id="ST_create_main" name="Створити головне завдання">')
    L(3, '<bpmn:extensionElements>')
    L(4, '<zeebe:taskDefinition type="http-request-smart" />')
    L(4, '<zeebe:ioMapping>')
    L(5, '<zeebe:input source="= &quot;POST&quot;" target="method" />')
    L(5, '<zeebe:input source="= &quot;https://o.tut.ua/web/hook/8531324a-2785-48d1-8f4d-ddd66a267d50&quot;" target="url" />')
    L(5, '<zeebe:input source="= {&quot;Content-Type&quot;:&quot;application/json&quot;}" target="headers" />')
    L(5, f'<zeebe:input source="= {{name: &quot;{_xml_escape(process_name)}&quot;, create_process: true, _model: &quot;project.project&quot;, _id: 252}}" target="body" />')
    L(4, '</zeebe:ioMapping>')
    L(3, '</bpmn:extensionElements>')
    L(3, '<bpmn:incoming>Flow_odoo_default</bpmn:incoming>')
    L(3, '<bpmn:outgoing>Flow_create_to_merge</bpmn:outgoing>')
    L(2, '</bpmn:serviceTask>')

    # --- GW_odoo_merge ---
    L(2, '<bpmn:exclusiveGateway id="GW_odoo_merge">')
    L(3, '<bpmn:incoming>Flow_odoo_skip</bpmn:incoming>')
    L(3, '<bpmn:incoming>Flow_create_to_merge</bpmn:incoming>')
    # Find what comes after start in the original graph
    start_elem = graph.get_start()
    first_flow_target = None
    if start_elem:
        outgoing = graph.get_outgoing(start_elem.id)
        if outgoing:
            first_flow_target = outgoing[0].to_id
    first_flow_id = 'Flow_merge_to_first'
    L(3, f'<bpmn:outgoing>{first_flow_id}</bpmn:outgoing>')
    L(2, '</bpmn:exclusiveGateway>')

    # --- Odoo check flows ---
    L(2, '<bpmn:sequenceFlow id="Flow_start_to_check" sourceRef="StartEvent_1" targetRef="GW_odoo_check" />')
    L(2, '<bpmn:sequenceFlow id="Flow_odoo_skip" sourceRef="GW_odoo_check" targetRef="GW_odoo_merge">')
    L(3, '<bpmn:conditionExpression xsi:type="bpmn:tFormalExpression">= is defined(odoo_task_id) and odoo_task_id != null</bpmn:conditionExpression>')
    L(2, '</bpmn:sequenceFlow>')
    L(2, '<bpmn:sequenceFlow id="Flow_odoo_default" sourceRef="GW_odoo_check" targetRef="ST_create_main" />')
    L(2, '<bpmn:sequenceFlow id="Flow_create_to_merge" sourceRef="ST_create_main" targetRef="GW_odoo_merge" />')

    # Flow from merge to first element
    if first_flow_target and first_flow_target in bpmn_ids:
        target_bid = bpmn_ids[first_flow_target]
        L(2, f'<bpmn:sequenceFlow id="{first_flow_id}" sourceRef="GW_odoo_merge" targetRef="{target_bid}" />')

    # --- PROCESS ELEMENTS (from .grs) ---
    # Track incoming/outgoing for each element
    elem_incoming = {}  # bpmn_id -> [flow_ids]
    elem_outgoing = {}  # bpmn_id -> [flow_ids]

    # Build incoming/outgoing maps
    for conn in graph.connections:
        if conn.from_id in bpmn_ids and conn.to_id in bpmn_ids:
            fid = flow_ids.get((conn.from_id, conn.to_id), f'Flow_{conn.line_id}')
            src_bid = bpmn_ids[conn.from_id]
            tgt_bid = bpmn_ids[conn.to_id]
            elem_outgoing.setdefault(src_bid, []).append(fid)
            elem_incoming.setdefault(tgt_bid, []).append(fid)

    # Add the merge→first flow
    if first_flow_target and first_flow_target in bpmn_ids:
        tgt = bpmn_ids[first_flow_target]
        elem_incoming.setdefault(tgt, []).append(first_flow_id)

    # Generate elements (skip start/end - handled separately)
    for eid in topo:
        elem = graph.elements[eid]
        bid = bpmn_ids[eid]
        incoming = elem_incoming.get(bid, [])
        outgoing = elem_outgoing.get(bid, [])

        if elem.type == 'start':
            continue  # Already generated

        elif elem.type == 'end':
            L(2, f'<bpmn:endEvent id="{bid}" name="Процес завершено">')
            for fid in incoming:
                L(3, f'<bpmn:incoming>{fid}</bpmn:incoming>')
            L(2, '</bpmn:endEvent>')

        elif elem.type == 'task':
            suffix = bid.replace('UT_', '')
            label = elem.label or elem.internal_name

            # User Task
            L(2, f'<bpmn:userTask id="{bid}" name="{_xml_escape(label)}">')
            L(3, '<bpmn:extensionElements>')
            L(4, '<zeebe:taskDefinition type="http-request-smart" />')
            L(4, '<zeebe:ioMapping>')
            L(5, '<zeebe:input source="= &quot;POST&quot;" target="method" />')
            L(5, '<zeebe:input source="= &quot;https://o.tut.ua/web/hook/8531324a-2785-48d1-8f4d-ddd66a267d50&quot;" target="url" />')
            L(5, '<zeebe:input source="= {&quot;Content-Type&quot;:&quot;application/json&quot;}" target="headers" />')
            L(5, f'<zeebe:input source="= {{name: &quot;{_xml_escape(label)}&quot;, description: &quot;&quot;, _model: &quot;project.project&quot;, _id: 252, process_instance_key: process_instance_key}}" target="body" />')
            L(4, '</zeebe:ioMapping>')
            L(3, '</bpmn:extensionElements>')
            for fid in incoming:
                L(3, f'<bpmn:incoming>{fid}</bpmn:incoming>')
            for fid in outgoing:
                L(3, f'<bpmn:outgoing>{fid}</bpmn:outgoing>')
            L(2, f'</bpmn:userTask>')

            # Boundary events
            L(2, f'<bpmn:boundaryEvent id="BE_rem_{suffix}" name="Нагадування" cancelActivity="false" attachedToRef="{bid}">')
            L(3, '<bpmn:outgoing>Flow_rem_{0}</bpmn:outgoing>'.format(suffix))
            L(3, '<bpmn:timerEventDefinition><bpmn:timeCycle xsi:type="bpmn:tFormalExpression">R/PT24H</bpmn:timeCycle></bpmn:timerEventDefinition>')
            L(2, '</bpmn:boundaryEvent>')

            L(2, f'<bpmn:boundaryEvent id="BE_ded_{suffix}" name="Дедлайн" cancelActivity="false" attachedToRef="{bid}">')
            L(3, '<bpmn:outgoing>Flow_ded_{0}</bpmn:outgoing>'.format(suffix))
            L(3, '<bpmn:timerEventDefinition><bpmn:timeDuration xsi:type="bpmn:tFormalExpression">P3D</bpmn:timeDuration></bpmn:timerEventDefinition>')
            L(2, '</bpmn:boundaryEvent>')

            # Reminder service task
            L(2, f'<bpmn:serviceTask id="ST_rem_{suffix}" name="⚠️ НАГАДУВАННЯ: {_xml_escape(label)}">')
            L(3, '<bpmn:extensionElements>')
            L(4, '<zeebe:taskDefinition type="http-request-smart" />')
            L(4, '<zeebe:ioMapping>')
            L(5, '<zeebe:input source="= &quot;POST&quot;" target="method" />')
            L(5, '<zeebe:input source="= &quot;https://o.tut.ua/web/hook/8531324a-2785-48d1-8f4d-ddd66a267d50&quot;" target="url" />')
            L(5, '<zeebe:input source="= {&quot;Content-Type&quot;:&quot;application/json&quot;}" target="headers" />')
            L(5, f'<zeebe:input source="= {{name: &quot;⚠️ НАГАДУВАННЯ: {_xml_escape(label)}&quot;, _model: &quot;project.project&quot;, _id: 252, process_instance_key: process_instance_key}}" target="body" />')
            L(4, '</zeebe:ioMapping>')
            L(3, '</bpmn:extensionElements>')
            L(3, f'<bpmn:incoming>Flow_rem_{suffix}</bpmn:incoming>')
            L(3, f'<bpmn:outgoing>Flow_rem_end_{suffix}</bpmn:outgoing>')
            L(2, '</bpmn:serviceTask>')

            L(2, f'<bpmn:endEvent id="End_rem_{suffix}">')
            L(3, f'<bpmn:incoming>Flow_rem_end_{suffix}</bpmn:incoming>')
            L(2, '</bpmn:endEvent>')

            # Escalation user task
            L(2, f'<bpmn:userTask id="UT_esc_{suffix}" name="🔴 ЕСКАЛАЦІЯ: {_xml_escape(label)}">')
            L(3, '<bpmn:extensionElements>')
            L(4, '<zeebe:taskDefinition type="http-request-smart" />')
            L(4, '<zeebe:ioMapping>')
            L(5, '<zeebe:input source="= &quot;POST&quot;" target="method" />')
            L(5, '<zeebe:input source="= &quot;https://o.tut.ua/web/hook/8531324a-2785-48d1-8f4d-ddd66a267d50&quot;" target="url" />')
            L(5, '<zeebe:input source="= {&quot;Content-Type&quot;:&quot;application/json&quot;}" target="headers" />')
            L(5, f'<zeebe:input source="= {{name: &quot;🔴 ЕСКАЛАЦІЯ: {_xml_escape(label)}&quot;, _model: &quot;project.project&quot;, _id: 252, process_instance_key: process_instance_key}}" target="body" />')
            L(4, '</zeebe:ioMapping>')
            L(3, '</bpmn:extensionElements>')
            L(3, f'<bpmn:incoming>Flow_ded_{suffix}</bpmn:incoming>')
            L(3, f'<bpmn:outgoing>Flow_esc_end_{suffix}</bpmn:outgoing>')
            L(2, '</bpmn:userTask>')

            L(2, f'<bpmn:endEvent id="End_esc_{suffix}">')
            L(3, f'<bpmn:incoming>Flow_esc_end_{suffix}</bpmn:incoming>')
            L(2, '</bpmn:endEvent>')

            # Reminder/escalation flows
            L(2, f'<bpmn:sequenceFlow id="Flow_rem_{suffix}" sourceRef="BE_rem_{suffix}" targetRef="ST_rem_{suffix}" />')
            L(2, f'<bpmn:sequenceFlow id="Flow_rem_end_{suffix}" sourceRef="ST_rem_{suffix}" targetRef="End_rem_{suffix}" />')
            L(2, f'<bpmn:sequenceFlow id="Flow_ded_{suffix}" sourceRef="BE_ded_{suffix}" targetRef="UT_esc_{suffix}" />')
            L(2, f'<bpmn:sequenceFlow id="Flow_esc_end_{suffix}" sourceRef="UT_esc_{suffix}" targetRef="End_esc_{suffix}" />')

        elif elem.type == 'gateway':
            label = elem.label or elem.internal_name
            # Find default flow (first outgoing, usually "Нет")
            default_flow = ''
            out_conns = graph.get_outgoing(eid)
            for c in out_conns:
                fid = flow_ids.get((c.from_id, c.to_id))
                if c.branch_index == 0 and fid:  # branch 0 = default ("Нет")
                    default_flow = fid
                    break

            attrs = f'id="{bid}" name="{_xml_escape(label)}"'
            if default_flow:
                attrs += f' default="{default_flow}"'
            L(2, f'<bpmn:exclusiveGateway {attrs}>')
            for fid in incoming:
                L(3, f'<bpmn:incoming>{fid}</bpmn:incoming>')
            for fid in outgoing:
                L(3, f'<bpmn:outgoing>{fid}</bpmn:outgoing>')
            L(2, '</bpmn:exclusiveGateway>')

        elif elem.type == 'parallel':
            # Parallel gateway (split)
            L(2, f'<bpmn:parallelGateway id="{bid}">')
            for fid in incoming:
                L(3, f'<bpmn:incoming>{fid}</bpmn:incoming>')
            for fid in outgoing:
                L(3, f'<bpmn:outgoing>{fid}</bpmn:outgoing>')
            L(2, '</bpmn:parallelGateway>')

    # --- SEQUENCE FLOWS (from .grs connections) ---
    for conn in graph.connections:
        if conn.from_id not in bpmn_ids or conn.to_id not in bpmn_ids:
            continue
        src = bpmn_ids[conn.from_id]
        tgt = bpmn_ids[conn.to_id]
        fid = flow_ids.get((conn.from_id, conn.to_id), f'Flow_{conn.line_id}')

        # Check if this is a conditional flow from a gateway
        src_elem = graph.elements.get(conn.from_id)
        if src_elem and src_elem.type == 'gateway' and conn.branch_index == 1:
            # "Да" branch - add condition
            L(2, f'<bpmn:sequenceFlow id="{fid}" sourceRef="{src}" targetRef="{tgt}">')
            # Use the gateway label as the variable name
            gw_label = src_elem.label
            var_name = f'x_studio_camunda_{slugify(gw_label)}'
            L(3, f'<bpmn:conditionExpression xsi:type="bpmn:tFormalExpression">= {var_name} = true</bpmn:conditionExpression>')
            L(2, '</bpmn:sequenceFlow>')
        else:
            L(2, f'<bpmn:sequenceFlow id="{fid}" sourceRef="{src}" targetRef="{tgt}" />')

    L(1, '</bpmn:process>')

    # ========== DIAGRAM ==========
    _generate_diagram(lines, graph, bpmn_ids, flow_ids, tasks, first_flow_id, first_flow_target)

    L(0, '</bpmn:definitions>')

    return '\n'.join(lines)


def _generate_diagram(lines, graph, bpmn_ids, flow_ids, tasks, first_flow_id, first_flow_target):
    """Generate BPMNDiagram section with layout."""

    def L(indent, text):
        lines.append('  ' * indent + text)

    # Layout constants
    POOL_X, POOL_Y = 160, 80
    LANE_SYS_H, LANE_RESP_H, LANE_MGR_H = 440, 300, 300
    POOL_H = LANE_SYS_H + LANE_RESP_H + LANE_MGR_H
    LANE_X = 190

    SYS_TOP = POOL_Y
    RESP_TOP = SYS_TOP + LANE_SYS_H
    MGR_TOP = RESP_TOP + LANE_RESP_H

    SYS_Y = SYS_TOP + 130
    SYS_REM_Y = SYS_TOP + 360
    RESP_Y = RESP_TOP + 150
    MGR_Y = MGR_TOP + 150

    # Calculate X positions based on topological order
    topo = graph.topo_order()
    x_pos = {}
    x_current = 300  # Start x after odoo check block

    # Odoo check elements
    odoo_positions = {
        'StartEvent_1': (220, SYS_Y),
        'GW_odoo_check': (330, SYS_Y),
        'ST_create_main': (490, SYS_Y),
        'GW_odoo_merge': (650, SYS_Y),
    }

    x_current = 830

    for eid in topo:
        elem = graph.elements[eid]
        bid = bpmn_ids.get(eid)
        if not bid:
            continue

        if elem.type == 'start':
            continue  # handled in odoo_positions

        # Determine Y based on element type
        if elem.type in ('gateway', 'parallel'):
            y = SYS_Y
        elif elem.type == 'task':
            y = RESP_Y
        elif elem.type == 'end':
            y = SYS_Y
        else:
            y = SYS_Y

        x_pos[bid] = (x_current, y)
        x_current += 200

    POOL_W = max(x_current + 100, 2000) - POOL_X
    LANE_W = POOL_W - 30

    # Start diagram
    L(1, '<bpmndi:BPMNDiagram id="BPMNDiagram_1">')
    L(2, '<bpmndi:BPMNPlane id="BPMNPlane_1" bpmnElement="Collaboration_1">')

    # Pool & Lanes
    L(3, f'<bpmndi:BPMNShape id="Participant_1_di" bpmnElement="Participant_1" isHorizontal="true">')
    L(4, f'<dc:Bounds x="{POOL_X}" y="{POOL_Y}" width="{POOL_W}" height="{POOL_H}" />')
    L(3, '</bpmndi:BPMNShape>')

    for lane_id, lane_y, lane_h in [
        ('Lane_system', SYS_TOP, LANE_SYS_H),
        ('Lane_responsible', RESP_TOP, LANE_RESP_H),
        ('Lane_manager', MGR_TOP, LANE_MGR_H)
    ]:
        L(3, f'<bpmndi:BPMNShape id="{lane_id}_di" bpmnElement="{lane_id}" isHorizontal="true">')
        L(4, f'<dc:Bounds x="{LANE_X}" y="{lane_y}" width="{LANE_W}" height="{lane_h}" />')
        L(3, '</bpmndi:BPMNShape>')

    def shape(bid, cx, cy, w, h):
        L(3, f'<bpmndi:BPMNShape id="{bid}_di" bpmnElement="{bid}">')
        L(4, f'<dc:Bounds x="{cx - w//2}" y="{cy - h//2}" width="{w}" height="{h}" />')
        L(3, '</bpmndi:BPMNShape>')

    # Odoo check shapes
    for bid, (cx, cy) in odoo_positions.items():
        if bid.startswith('GW_'):
            shape(bid, cx, cy, 50, 50)
        elif bid.startswith('ST_'):
            shape(bid, cx, cy, 100, 80)
        else:
            shape(bid, cx, cy, 36, 36)

    # Main element shapes
    for eid in topo:
        elem = graph.elements[eid]
        bid = bpmn_ids.get(eid)
        if not bid or bid in odoo_positions:
            continue
        if bid not in x_pos:
            continue
        cx, cy = x_pos[bid]

        if elem.type in ('start', 'end'):
            shape(bid, cx, cy, 36, 36)
        elif elem.type in ('gateway', 'parallel'):
            shape(bid, cx, cy, 50, 50)
        elif elem.type == 'task':
            shape(bid, cx, cy, 100, 80)
            # Boundary events
            suffix = bid.replace('UT_', '')
            shape(f'BE_rem_{suffix}', cx - 30, cy + 22, 36, 36)
            shape(f'BE_ded_{suffix}', cx + 30, cy + 22, 36, 36)
            # Reminder ST
            shape(f'ST_rem_{suffix}', cx, SYS_REM_Y, 100, 80)
            shape(f'End_rem_{suffix}', cx + 90, SYS_REM_Y, 36, 36)
            # Escalation UT
            shape(f'UT_esc_{suffix}', cx, MGR_Y, 100, 80)
            shape(f'End_esc_{suffix}', cx + 90, MGR_Y, 36, 36)

    def edge(fid, sx, sy, tx, ty):
        L(3, f'<bpmndi:BPMNEdge id="{fid}_di" bpmnElement="{fid}">')
        L(4, f'<di:waypoint x="{sx}" y="{sy}" />')
        L(4, f'<di:waypoint x="{tx}" y="{ty}" />')
        L(3, '</bpmndi:BPMNEdge>')

    # Odoo check edges
    edge('Flow_start_to_check', 238, SYS_Y, 305, SYS_Y)
    edge('Flow_odoo_default', 355, SYS_Y, 440, SYS_Y)
    edge('Flow_odoo_skip', 330, SYS_Y - 25, 650, SYS_Y - 25)
    edge('Flow_create_to_merge', 540, SYS_Y, 625, SYS_Y)

    if first_flow_target and first_flow_target in bpmn_ids:
        tbid = bpmn_ids[first_flow_target]
        if tbid in x_pos:
            tx, ty = x_pos[tbid]
            edge(first_flow_id, 675, SYS_Y, tx - 50 if ty == SYS_Y else tx, ty - 25 if ty != SYS_Y else ty)

    # Main flow edges
    for conn in graph.connections:
        if conn.from_id not in bpmn_ids or conn.to_id not in bpmn_ids:
            continue
        src = bpmn_ids[conn.from_id]
        tgt = bpmn_ids[conn.to_id]
        fid = flow_ids.get((conn.from_id, conn.to_id))
        if not fid:
            continue

        # Get positions
        src_pos = x_pos.get(src) or odoo_positions.get(src)
        tgt_pos = x_pos.get(tgt) or odoo_positions.get(tgt)
        if not src_pos or not tgt_pos:
            continue

        sx, sy = src_pos
        tx, ty = tgt_pos

        # Simple 2-waypoint edge
        edge(fid, sx + 50, sy, tx - 50, ty)

    # Boundary event edges
    for eid, elem in tasks.items():
        bid = bpmn_ids[eid]
        suffix = bid.replace('UT_', '')
        pos = x_pos.get(bid)
        if not pos:
            continue
        cx, cy = pos

        edge(f'Flow_rem_{suffix}', cx - 30, cy + 40, cx - 50, SYS_REM_Y)
        edge(f'Flow_rem_end_{suffix}', cx + 50, SYS_REM_Y, cx + 72, SYS_REM_Y)
        edge(f'Flow_ded_{suffix}', cx + 30, cy + 40, cx - 50, MGR_Y)
        edge(f'Flow_esc_end_{suffix}', cx + 50, MGR_Y, cx + 72, MGR_Y)

    L(2, '</bpmndi:BPMNPlane>')
    L(1, '</bpmndi:BPMNDiagram>')


def _xml_escape(text: str) -> str:
    """Escape special XML characters."""
    return (text
            .replace('&', '&amp;')
            .replace('<', '&lt;')
            .replace('>', '&gt;')
            .replace('"', '&quot;')
            .replace("'", '&apos;'))


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description='Convert GRAPES (.grs) file to Camunda 8.8 BPMN',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='Example: python3 grs_to_bpmn.py process.grs output.bpmn --name "Мій процес"'
    )
    parser.add_argument('input', help='Input .grs file path')
    parser.add_argument('output', help='Output .bpmn file path')
    parser.add_argument('--name', '-n', required=True, help='Process name (Ukrainian)')
    parser.add_argument('--lane', '-l', default='Бухгалтер', help='Main executor lane name (default: Бухгалтер)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Show parsed structure')

    args = parser.parse_args()

    # Read input
    with open(args.input, 'r', encoding='utf-8') as f:
        content = f.read()

    # Tokenize
    tokens = tokenize(content)
    print(f'Tokenized: {len(tokens)} tokens')

    # Parse
    p = Parser(tokens)
    parsed = p.parse()
    print('Parsed: OK')

    # Extract
    graph = extract_process(parsed)
    print(f'Extracted: {len(graph.elements)} elements, {len(graph.connections)} connections')

    if args.verbose:
        print('\n--- Elements ---')
        for eid, elem in sorted(graph.elements.items()):
            print(f'  [{elem.type:8s}] id={eid:3d}  label="{elem.label}"  name="{elem.internal_name}"')
        print('\n--- Connections ---')
        for conn in graph.connections:
            label = f' [{conn.label}]' if conn.label else ''
            print(f'  {conn.from_id} -> {conn.to_id}{label}  (branch={conn.branch_index})')

    # Generate BPMN
    bpmn = generate_bpmn(graph, args.name, args.lane)

    # Write output
    with open(args.output, 'w', encoding='utf-8') as f:
        f.write(bpmn)

    # Validate
    import xml.etree.ElementTree as ET
    try:
        ET.fromstring(bpmn)
        print(f'\nOutput: {args.output} (valid XML)')
    except ET.ParseError as e:
        print(f'\nWARNING: XML validation failed: {e}')

    print('Done!')


if __name__ == '__main__':
    main()
