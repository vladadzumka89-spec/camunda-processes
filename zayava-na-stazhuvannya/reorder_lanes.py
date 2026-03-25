#!/usr/bin/env python3
"""
Reorder swim lanes in zayava-na-stazhuvannya-v2.bpmn.

Move Lane_Avtomatyzator from position 8 (last) to position 2 (after Kerivnyk).

Uses xml.etree.ElementTree for parsing to build lane mappings,
but applies coordinate changes via regex to preserve comments, formatting, etc.
"""

import xml.etree.ElementTree as ET
import re
import sys
import os
import copy

BPMN_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "zayava-na-stazhuvannya-v2.bpmn")

# Namespaces for ET parsing
NS = {
    'bpmn': 'http://www.omg.org/spec/BPMN/20100524/MODEL',
    'bpmndi': 'http://www.omg.org/spec/BPMN/20100524/DI',
    'dc': 'http://www.omg.org/spec/DD/20100524/DC',
    'di': 'http://www.omg.org/spec/DD/20100524/DI',
}

# Register all namespaces
for prefix, uri in NS.items():
    ET.register_namespace(prefix, uri)
ET.register_namespace('modeler', 'http://camunda.org/schema/modeler/1.0')
ET.register_namespace('zeebe', 'http://camunda.org/schema/zeebe/1.0')
ET.register_namespace('xsi', 'http://www.w3.org/2001/XMLSchema-instance')

# Lane shape IDs (same as bpmnElement IDs for lane shapes)
LANE_SHAPE_IDS = {
    'Lane_Kerivnyk', 'Lane_Rekruter', 'Lane_Revizor', 'Lane_Pracivnyk',
    'Lane_Finansyst', 'Lane_KerLogistyky', 'Lane_Administrator', 'Lane_Avtomatyzator',
}

POOL_ID = 'Participant_1'


def get_y_offset_for_old_zone(y):
    """Given a y coordinate in the OLD layout, return the Y offset."""
    if y < 260:
        return 0        # Kerivnyk zone
    elif y < 920:
        return 440      # Rekruter/Revizor/Pracivnyk/Finansyst/KerLogistyky/Administrator
    else:
        return -660     # Avtomatyzator


def format_y(val):
    """Format a y value: use int if it's a whole number."""
    if val == int(val):
        return str(int(val))
    return str(val)


def main():
    print(f"Reading BPMN file: {BPMN_FILE}")

    # Read original content
    with open(BPMN_FILE, 'r', encoding='utf-8') as f:
        original_content = f.read()

    # Parse with ET to build element-to-lane mapping
    tree = ET.parse(BPMN_FILE)
    root = tree.getroot()

    # ===== Build element-to-lane mapping =====
    element_to_lane = {}
    lane_set = root.find('.//bpmn:laneSet', NS)
    for lane in lane_set.findall('bpmn:lane', NS):
        lane_id = lane.get('id')
        for ref in lane.findall('bpmn:flowNodeRef', NS):
            element_to_lane[ref.text] = lane_id

    # Add boundary events
    for be in root.iter('{http://www.omg.org/spec/BPMN/20100524/MODEL}boundaryEvent'):
        be_id = be.get('id')
        attached = be.get('attachedToRef')
        if attached and attached in element_to_lane:
            element_to_lane[be_id] = element_to_lane[attached]

    print(f"  Built lane mapping: {len(element_to_lane)} elements")

    # ===== Now do all modifications on the raw XML string =====
    content = original_content

    # ---------- STEP 1: Update lane shape positions ----------
    # Pattern: <bpmndi:BPMNShape id="XXX_di" bpmnElement="Lane_XXX" isHorizontal="true">
    #            <dc:Bounds x="182" y="920" width="5570" height="440" />
    lane_new_positions = {
        'Lane_Kerivnyk':     (60,   200),
        'Lane_Avtomatyzator': (260,  440),
        'Lane_Rekruter':      (700,  100),
        'Lane_Revizor':       (800,  100),
        'Lane_Pracivnyk':     (900,  100),
        'Lane_Finansyst':     (1000, 120),
        'Lane_KerLogistyky':  (1120, 120),
        'Lane_Administrator': (1240, 120),
    }

    for lane_id, (new_y, new_h) in lane_new_positions.items():
        # Find the BPMNShape for this lane and update its Bounds
        pattern = (
            r'(<bpmndi:BPMNShape\s+id="' + re.escape(lane_id) + r'_di"\s+bpmnElement="' +
            re.escape(lane_id) + r'"\s+isHorizontal="true">\s*'
            r'<dc:Bounds\s+x="(\d+)"\s+y=")(\d+)("\s+width="(\d+)"\s+height=")(\d+)(")'
        )
        match = re.search(pattern, content)
        if match:
            old_y = match.group(3)
            old_h = match.group(6)
            replacement = match.group(1) + str(new_y) + match.group(4) + str(new_h) + match.group(7)
            content = content[:match.start()] + replacement + content[match.end():]
            print(f"  LANE {lane_id}: y={old_y}->{new_y}, h={old_h}->{new_h}")
        else:
            print(f"  WARNING: Could not find lane shape for {lane_id}")

    # ---------- STEP 2: Update element shapes ----------
    # We need to process each BPMNShape that is NOT a lane and NOT the pool
    # Pattern for BPMNShape with Bounds:
    #   <bpmndi:BPMNShape id="..." bpmnElement="ELEMENT_ID" ...>
    #     <dc:Bounds x="..." y="..." width="..." height="..." />

    # First, collect all BPMNShape blocks with their positions
    shape_pattern = re.compile(
        r'<bpmndi:BPMNShape\s+id="([^"]+)"\s+bpmnElement="([^"]+)"[^>]*>'
    )

    # Process shapes one at a time, tracking position changes
    pos = 0
    changes = []

    for match in shape_pattern.finditer(content):
        shape_id = match.group(1)
        bpmn_element = match.group(2)

        # Skip pool and lanes
        if bpmn_element == POOL_ID or bpmn_element in LANE_SHAPE_IDS:
            continue

        # Determine offset for this element
        lane_id = element_to_lane.get(bpmn_element)
        if lane_id:
            if lane_id == 'Lane_Kerivnyk':
                offset = 0
            elif lane_id == 'Lane_Avtomatyzator':
                offset = -660
            else:
                offset = 440
        else:
            offset = None  # Will use zone-based for text annotations

        if offset == 0 and lane_id:
            continue  # No change needed

        # Find the closing </bpmndi:BPMNShape> tag
        shape_start = match.start()
        close_tag = '</bpmndi:BPMNShape>'
        # Could also be self-closing, but BPMNShapes have children
        shape_end_idx = content.find(close_tag, shape_start)
        if shape_end_idx == -1:
            continue
        shape_end_idx += len(close_tag)

        shape_block = content[shape_start:shape_end_idx]

        changes.append((shape_start, shape_end_idx, bpmn_element, offset, shape_block))

    # Apply changes in reverse order to maintain positions
    for shape_start, shape_end_idx, bpmn_element, offset, shape_block in reversed(changes):
        new_block = shape_block

        # Update all dc:Bounds y values in this shape block
        def replace_bounds_y(m):
            old_y = float(m.group(2))
            eff_offset = offset if offset is not None else get_y_offset_for_old_zone(old_y)
            if eff_offset == 0:
                return m.group(0)
            new_y = old_y + eff_offset
            return m.group(1) + format_y(new_y) + m.group(3)

        new_block = re.sub(
            r'(<dc:Bounds\s+x="[^"]+"\s+y=")([^"]+)("\s+width="[^"]+"\s+height="[^"]+")',
            replace_bounds_y,
            new_block
        )

        if new_block != shape_block:
            content = content[:shape_start] + new_block + content[shape_end_idx:]
            print(f"  SHAPE {bpmn_element}: updated (offset={offset})")

    # ---------- STEP 3: Update edge waypoints ----------
    edge_pattern = re.compile(
        r'<bpmndi:BPMNEdge\s+id="([^"]+)"\s+bpmnElement="([^"]+)"[^>]*>'
    )

    edge_changes = []
    for match in edge_pattern.finditer(content):
        edge_id = match.group(1)
        edge_element = match.group(2)

        edge_start = match.start()
        close_tag = '</bpmndi:BPMNEdge>'
        edge_end_idx = content.find(close_tag, edge_start)
        if edge_end_idx == -1:
            continue
        edge_end_idx += len(close_tag)

        edge_block = content[edge_start:edge_end_idx]
        edge_changes.append((edge_start, edge_end_idx, edge_element, edge_block))

    wp_count = 0
    for edge_start, edge_end_idx, edge_element, edge_block in reversed(edge_changes):
        new_block = edge_block

        # Update waypoint y values
        def replace_wp_y(m):
            nonlocal wp_count
            old_y = float(m.group(1))
            eff_offset = get_y_offset_for_old_zone(old_y)
            if eff_offset == 0:
                return m.group(0)
            new_y = old_y + eff_offset
            wp_count += 1
            return '<di:waypoint x="' + m.group(0).split('x="')[1].split('"')[0] + '" y="' + format_y(new_y) + '"'

        # More precise waypoint replacement
        def replace_wp(m):
            nonlocal wp_count
            x_val = m.group(1)
            old_y = float(m.group(2))
            eff_offset = get_y_offset_for_old_zone(old_y)
            if eff_offset == 0:
                return m.group(0)
            new_y = old_y + eff_offset
            wp_count += 1
            return f'<di:waypoint x="{x_val}" y="{format_y(new_y)}"'

        new_block = re.sub(
            r'<di:waypoint\s+x="([^"]+)"\s+y="([^"]+)"',
            replace_wp,
            new_block
        )

        # Update label y values in edges (dc:Bounds inside BPMNLabel)
        def replace_label_bounds_y(m):
            old_y = float(m.group(2))
            eff_offset = get_y_offset_for_old_zone(old_y)
            if eff_offset == 0:
                return m.group(0)
            new_y = old_y + eff_offset
            return m.group(1) + format_y(new_y) + m.group(3)

        new_block = re.sub(
            r'(<dc:Bounds\s+x="[^"]+"\s+y=")([^"]+)("\s+width="[^"]+"\s+height="[^"]+")',
            replace_label_bounds_y,
            new_block
        )

        if new_block != edge_block:
            content = content[:edge_start] + new_block + content[edge_end_idx:]

    print(f"  Updated {wp_count} edge waypoints")

    # ---------- STEP 4: Reorder lane elements in laneSet XML ----------
    # Find the laneSet block
    laneset_start_match = re.search(r'<bpmn:laneSet\s+id="LaneSet_1">', content)
    laneset_end_match = re.search(r'</bpmn:laneSet>', content)

    if laneset_start_match and laneset_end_match:
        ls_start = laneset_start_match.start()
        ls_end = laneset_end_match.end()
        laneset_block = content[ls_start:ls_end]

        # Extract each lane block
        lane_blocks = {}
        lane_pattern = re.compile(
            r'(\s*<bpmn:lane\s+id="([^"]+)"[^>]*>.*?</bpmn:lane>)',
            re.DOTALL
        )
        for m in lane_pattern.finditer(laneset_block):
            lane_blocks[m.group(2)] = m.group(1)

        desired_order = [
            'Lane_Kerivnyk',
            'Lane_Avtomatyzator',
            'Lane_Rekruter',
            'Lane_Revizor',
            'Lane_Pracivnyk',
            'Lane_Finansyst',
            'Lane_KerLogistyky',
            'Lane_Administrator',
        ]

        new_laneset = '<bpmn:laneSet id="LaneSet_1">'
        for lane_id in desired_order:
            if lane_id in lane_blocks:
                new_laneset += lane_blocks[lane_id]
        new_laneset += '\n    </bpmn:laneSet>'

        content = content[:ls_start] + new_laneset + content[ls_end:]
        print("  Reordered lanes in laneSet")
    else:
        print("  WARNING: Could not find laneSet block")

    # ---------- STEP 5: Reorder lane shapes in BPMNPlane ----------
    # Find all lane shape blocks and reorder them after the pool shape
    # Find the pool shape block end
    pool_shape_match = re.search(
        r'(<bpmndi:BPMNShape\s+id="Participant_1_di".*?</bpmndi:BPMNShape>)',
        content, re.DOTALL
    )

    if pool_shape_match:
        # Extract all lane shape blocks
        lane_shape_blocks = {}
        for lane_id in LANE_SHAPE_IDS:
            pattern = re.compile(
                r'\s*<bpmndi:BPMNShape\s+id="' + re.escape(lane_id) +
                r'_di"\s+bpmnElement="' + re.escape(lane_id) + r'".*?</bpmndi:BPMNShape>',
                re.DOTALL
            )
            m = pattern.search(content)
            if m:
                lane_shape_blocks[lane_id] = m.group(0)

        # Remove all lane shape blocks from content
        for lane_id in LANE_SHAPE_IDS:
            pattern = re.compile(
                r'\s*<bpmndi:BPMNShape\s+id="' + re.escape(lane_id) +
                r'_di"\s+bpmnElement="' + re.escape(lane_id) + r'".*?</bpmndi:BPMNShape>',
                re.DOTALL
            )
            content = pattern.sub('', content, count=1)

        # Find pool shape end position again (it may have shifted)
        pool_shape_match = re.search(
            r'(<bpmndi:BPMNShape\s+id="Participant_1_di".*?</bpmndi:BPMNShape>)',
            content, re.DOTALL
        )
        insert_pos = pool_shape_match.end()

        # Build lane shapes string in desired order
        desired_order = [
            'Lane_Kerivnyk',
            'Lane_Avtomatyzator',
            'Lane_Rekruter',
            'Lane_Revizor',
            'Lane_Pracivnyk',
            'Lane_Finansyst',
            'Lane_KerLogistyky',
            'Lane_Administrator',
        ]

        lane_shapes_str = ''
        for lane_id in desired_order:
            if lane_id in lane_shape_blocks:
                lane_shapes_str += lane_shape_blocks[lane_id]

        content = content[:insert_pos] + lane_shapes_str + content[insert_pos:]
        print("  Reordered lane shapes in BPMNPlane")

    # ---------- STEP 6: Save ----------
    with open(BPMN_FILE, 'w', encoding='utf-8') as f:
        f.write(content)

    print(f"\n  File saved: {BPMN_FILE}")
    print("  Done!")


if __name__ == '__main__':
    main()
