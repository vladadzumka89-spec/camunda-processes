#!/usr/bin/env python3
"""
BPMN Auto Layout — Sugiyama hierarchical layout for BPMN diagrams.

Reads a BPMN file, computes a clean layout using a layered graph algorithm,
and writes the result back with a new <bpmndi:BPMNDiagram> section.

Steps:
1. Parse semantic model (elements, sequence flows, boundary events, lanes)
2. Build adjacency graph from sequence flows
3. Assign layers via BFS longest-path from start events
4. Read lane assignments from <bpmn:lane>/<bpmn:flowNodeRef>
5. Minimize crossings (barycenter heuristic, 3 sweeps)
6. Compute coordinates (X by layer, Y by lane)
7. Route edges orthogonally with collision avoidance
8. Write new DI section, removing the old one
"""

import xml.etree.ElementTree as ET
import argparse
import copy
import math
import sys
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Optional

from bpmn_layout_checker import (
    Rect, Shape, Edge, NS, parse_bpmn, line_intersects_rect, run_all_checks,
)

# ─── Layout constants ───────────────────────────────────────────────────────

# Element dimensions
EVENT_W, EVENT_H = 36, 36
GATEWAY_W, GATEWAY_H = 50, 50
TASK_W, TASK_H = 100, 80
BOUNDARY_W, BOUNDARY_H = 36, 36

# Grid (compact, matching reference Camunda Modeler style)
POOL_X, POOL_Y = 105, 90
LANE_LABEL_WIDTH = 30
LANE_X = POOL_X + LANE_LABEL_WIDTH  # 135
H_SPACING = 80   # min horizontal gap (compact but room for collision avoidance)
V_PADDING = 30   # vertical padding from lane edge to element
MIN_LANE_HEIGHT_EVENTS = 130
MIN_LANE_HEIGHT_TASKS = 170

# Boundary event offsets relative to parent task
# Reminder: left edge at task_x + 10, right edge at task_x + 46
# Deadline: left edge at task_x + 54, right edge at task_x + 90
# 8px gap between them (no overlap)
BE_REMINDER_DX = 10
BE_DEADLINE_DX = 54  # from task left edge
BE_DY_FROM_BOTTOM = 18


# ─── Semantic model ─────────────────────────────────────────────────────────

@dataclass
class BpmnElement:
    id: str
    tag: str  # e.g. 'startEvent', 'serviceTask', 'exclusiveGateway', etc.
    name: str = ""
    attached_to: Optional[str] = None  # for boundary events
    is_boundary_event: bool = False


@dataclass
class SequenceFlow:
    id: str
    source_ref: str
    target_ref: str


def element_size(tag: str):
    """Return (width, height) for a BPMN element type."""
    if 'Event' in tag or tag == 'startEvent' or tag == 'endEvent':
        return EVENT_W, EVENT_H
    if 'Gateway' in tag or 'gateway' in tag:
        return GATEWAY_W, GATEWAY_H
    return TASK_W, TASK_H


def parse_semantic_model(root):
    """Parse BPMN semantic model: elements, flows, boundary events, lanes, process, collaboration."""
    elements = {}       # id -> BpmnElement
    flows = []          # list of SequenceFlow
    boundary_map = {}   # boundary_id -> parent_id
    lane_map = {}       # element_id -> lane_id
    lane_order = []     # ordered list of lane_ids
    lane_names = {}     # lane_id -> name
    process_id = None
    collab_id = None
    participant_id = None
    pool_name = None

    bpmn_ns = NS['bpmn']

    # Find collaboration
    for collab in root.iter(f'{{{bpmn_ns}}}collaboration'):
        collab_id = collab.get('id')
        for part in collab.findall(f'{{{bpmn_ns}}}participant'):
            participant_id = part.get('id')
            pool_name = part.get('name', '')
            process_id = part.get('processRef')
        break

    # Find process
    for proc in root.iter(f'{{{bpmn_ns}}}process'):
        if process_id is None:
            process_id = proc.get('id')

        # Parse lanes
        for laneset in proc.findall(f'{{{bpmn_ns}}}laneSet'):
            for lane in laneset.findall(f'{{{bpmn_ns}}}lane'):
                lane_id = lane.get('id')
                lane_name = lane.get('name', lane_id)
                lane_order.append(lane_id)
                lane_names[lane_id] = lane_name
                for ref in lane.findall(f'{{{bpmn_ns}}}flowNodeRef'):
                    if ref.text:
                        lane_map[ref.text.strip()] = lane_id

        # Parse all flow nodes
        flow_node_tags = [
            'startEvent', 'endEvent', 'intermediateThrowEvent', 'intermediateCatchEvent',
            'serviceTask', 'userTask', 'businessRuleTask', 'scriptTask', 'sendTask', 'receiveTask',
            'callActivity', 'subProcess', 'task',
            'exclusiveGateway', 'parallelGateway', 'inclusiveGateway', 'eventBasedGateway',
            'boundaryEvent',
        ]

        for tag in flow_node_tags:
            for el in proc.iter(f'{{{bpmn_ns}}}{tag}'):
                el_id = el.get('id')
                el_name = el.get('name', '')
                attached = el.get('attachedToRef')
                is_be = tag == 'boundaryEvent'
                elements[el_id] = BpmnElement(
                    id=el_id, tag=tag, name=el_name,
                    attached_to=attached, is_boundary_event=is_be,
                )
                if is_be and attached:
                    boundary_map[el_id] = attached

        # Also pick up timerIntermediateCatchEvent / messageIntermediateCatchEvent
        for el in proc.iter(f'{{{bpmn_ns}}}intermediateCatchEvent'):
            el_id = el.get('id')
            if el_id not in elements:
                elements[el_id] = BpmnElement(id=el_id, tag='intermediateCatchEvent', name=el.get('name', ''))

        # Parse sequence flows
        for sf in proc.iter(f'{{{bpmn_ns}}}sequenceFlow'):
            sf_id = sf.get('id')
            src = sf.get('sourceRef')
            tgt = sf.get('targetRef')
            if sf_id and src and tgt:
                flows.append(SequenceFlow(id=sf_id, source_ref=src, target_ref=tgt))

    return {
        'elements': elements,
        'flows': flows,
        'boundary_map': boundary_map,
        'lane_map': lane_map,
        'lane_order': lane_order,
        'lane_names': lane_names,
        'process_id': process_id,
        'collab_id': collab_id,
        'participant_id': participant_id,
        'pool_name': pool_name,
    }


# ─── Graph + Layer assignment (Sugiyama step 1-2) ───────────────────────────

def build_graph(elements, flows, boundary_map):
    """Build adjacency lists, excluding boundary events from main graph."""
    adj = defaultdict(list)      # forward edges
    adj_rev = defaultdict(list)  # reverse edges
    flow_elements = set()

    for f in flows:
        src, tgt = f.source_ref, f.target_ref
        adj[src].append(tgt)
        adj_rev[tgt].append(src)
        flow_elements.add(src)
        flow_elements.add(tgt)

    return adj, adj_rev, flow_elements


def assign_layers(elements, adj, adj_rev, boundary_map):
    """BFS longest-path layer assignment from start events.
    Handles cycles by capping visit count per node.
    Boundary events get the same layer as their parent."""
    layers = {}

    # Find start nodes (no incoming edges, not boundary events)
    all_nodes = set(adj.keys()) | set(adj_rev.keys())
    start_nodes = [n for n in all_nodes
                   if n not in adj_rev or len(adj_rev[n]) == 0]

    # If no clear start, look for startEvent elements
    if not start_nodes:
        start_nodes = [eid for eid, el in elements.items()
                       if el.tag == 'startEvent']

    # BFS longest-path with visit cap to handle cycles
    queue = deque()
    visit_count = defaultdict(int)
    max_visits = 2  # allow re-visit once to propagate longer paths, but no more

    for s in start_nodes:
        if s not in boundary_map:
            layers[s] = 0
            queue.append(s)
            visit_count[s] = 1

    while queue:
        node = queue.popleft()
        for nxt in adj.get(node, []):
            if nxt in boundary_map:
                continue  # skip boundary events
            new_layer = layers[node] + 1
            if nxt not in layers or layers[nxt] < new_layer:
                if visit_count[nxt] < max_visits:
                    layers[nxt] = new_layer
                    visit_count[nxt] += 1
                    queue.append(nxt)

    # Assign boundary events to same layer as parent
    for be_id, parent_id in boundary_map.items():
        if parent_id in layers:
            layers[be_id] = layers[parent_id]

    # Propagate layers from boundary events to their downstream chain
    # (e.g., boundary_event → ST_rem_xxx → End_rem_xxx)
    be_queue = deque()
    for be_id in boundary_map:
        if be_id in layers:
            be_queue.append(be_id)

    while be_queue:
        node = be_queue.popleft()
        for nxt in adj.get(node, []):
            if nxt in boundary_map:
                continue
            new_layer = layers[node] + 1
            if nxt not in layers or layers[nxt] < new_layer:
                layers[nxt] = new_layer
                be_queue.append(nxt)

    return layers


# ─── Crossing minimization (Sugiyama step 3) ────────────────────────────────

def barycenter_ordering(layers_dict, adj, lane_map, lane_order):
    """Group nodes by layer, then within each layer sort by barycenter + lane.
    Returns dict: layer_num -> ordered list of node ids."""
    # Group by layer
    by_layer = defaultdict(list)
    for node, layer in layers_dict.items():
        by_layer[layer].append(node)

    max_layer = max(by_layer.keys()) if by_layer else 0

    # Lane index for vertical ordering
    lane_idx = {lid: i for i, lid in enumerate(lane_order)} if lane_order else {}

    def node_sort_key(node, prev_positions):
        """Sort key: (lane_index, barycenter_position)."""
        lid = lane_map.get(node, '')
        li = lane_idx.get(lid, 999)
        # Barycenter from predecessor positions
        preds = [p for p in (adj.get(node) or []) if p in prev_positions]
        # Actually use reverse: positions of predecessors of this node
        bc = 0
        return (li, bc)

    # Initial ordering by lane
    for layer in sorted(by_layer.keys()):
        by_layer[layer].sort(key=lambda n: lane_idx.get(lane_map.get(n, ''), 999))

    # 3 sweeps of barycenter
    for sweep in range(3):
        positions = {}  # node -> position index

        # Forward sweep (layer 0..max)
        if sweep % 2 == 0:
            layer_range = range(max_layer + 1)
        else:
            layer_range = range(max_layer, -1, -1)

        for layer in layer_range:
            nodes = by_layer[layer]

            if positions:  # not first layer
                def bc_key(n):
                    li = lane_idx.get(lane_map.get(n, ''), 999)
                    preds_in_prev = []
                    # Collect all connected nodes in adjacent layers
                    for other_node, pos in positions.items():
                        if other_node in (adj.get(n, [])):
                            preds_in_prev.append(pos)
                    # reverse adj too
                    for f_src, f_targets in adj.items():
                        if n in f_targets and f_src in positions:
                            preds_in_prev.append(positions[f_src])
                    bc = sum(preds_in_prev) / len(preds_in_prev) if preds_in_prev else 0
                    return (li, bc)

                nodes.sort(key=bc_key)

            for idx, n in enumerate(nodes):
                positions[n] = idx

    return dict(by_layer)


# ─── Coordinate assignment (Sugiyama step 4) ────────────────────────────────

def compute_coordinates(layers_by_num, elements, boundary_map, lane_map,
                        lane_order, verbose=False):
    """Assign (x, y, w, h) to each element. Returns positions dict and lane geometry."""

    positions = {}  # element_id -> (x, y, w, h)

    # Count elements per lane per layer to compute lane heights
    lane_rows = defaultdict(int)   # lane_id -> max elements in any column
    lane_has_task = defaultdict(bool)

    for layer_num, nodes in layers_by_num.items():
        lane_count = defaultdict(int)
        for n in nodes:
            if n in boundary_map:
                continue
            lid = lane_map.get(n, '_default')
            lane_count[lid] += 1
            tag = elements[n].tag if n in elements else ''
            if 'Task' in tag or 'task' in tag or 'Activity' in tag or 'activity' in tag or 'SubProcess' in tag:
                lane_has_task[lid] = True
        for lid, cnt in lane_count.items():
            lane_rows[lid] = max(lane_rows[lid], cnt)

    # Compute lane heights
    effective_lanes = lane_order if lane_order else ['_default']
    lane_heights = {}
    for lid in effective_lanes:
        rows = max(1, lane_rows.get(lid, 1))
        min_h = MIN_LANE_HEIGHT_TASKS if lane_has_task.get(lid) else MIN_LANE_HEIGHT_EVENTS
        lane_heights[lid] = max(min_h, rows * (TASK_H + 40) + 2 * V_PADDING)

    # Lane Y positions (stacked vertically)
    lane_y = {}
    current_y = POOL_Y
    for lid in effective_lanes:
        lane_y[lid] = current_y
        current_y += lane_heights[lid]

    total_pool_height = current_y - POOL_Y

    # Compute X positions per layer
    max_layer = max(layers_by_num.keys()) if layers_by_num else 0
    layer_x = {}
    current_x = LANE_X + 40  # start after lane label

    for layer_num in range(max_layer + 1):
        layer_x[layer_num] = current_x
        # Find widest element in this layer
        max_w = 0
        for n in layers_by_num.get(layer_num, []):
            if n in boundary_map:
                continue
            tag = elements[n].tag if n in elements else ''
            w, _ = element_size(tag)
            max_w = max(max_w, w)
        current_x += max(max_w, EVENT_W) + H_SPACING

    pool_width = current_x + 30 - POOL_X

    # Place each node
    for layer_num, nodes in layers_by_num.items():
        # Group nodes in this layer by lane
        lane_groups = defaultdict(list)
        for n in nodes:
            if n in boundary_map:
                continue  # place boundary events after
            lid = lane_map.get(n, effective_lanes[0] if effective_lanes else '_default')
            lane_groups[lid].append(n)

        x_base = layer_x[layer_num]

        for lid in effective_lanes:
            group = lane_groups.get(lid, [])
            if not group:
                continue

            ly = lane_y[lid]
            lh = lane_heights[lid]

            # Distribute vertically within lane
            total_needed = 0
            sizes = []
            for n in group:
                tag = elements[n].tag if n in elements else ''
                w, h = element_size(tag)
                sizes.append((w, h))
                total_needed += h

            spacing = 20
            total_with_spacing = total_needed + spacing * (len(group) - 1) if len(group) > 1 else total_needed
            start_y = ly + V_PADDING + max(0, (lh - 2 * V_PADDING - total_with_spacing) / 2)

            for idx, n in enumerate(group):
                w, h = sizes[idx]
                tag = elements[n].tag if n in elements else ''
                # Center smaller elements horizontally within the layer column
                max_w_in_layer = TASK_W
                for nn in layers_by_num.get(layer_num, []):
                    if nn not in boundary_map:
                        ttag = elements[nn].tag if nn in elements else ''
                        ww, _ = element_size(ttag)
                        max_w_in_layer = max(max_w_in_layer, ww)
                x = x_base + (max_w_in_layer - w) / 2
                y = start_y
                positions[n] = (x, y, w, h)
                start_y += h + spacing

    # Place boundary events on parent border
    for be_id, parent_id in boundary_map.items():
        if parent_id not in positions:
            continue
        px, py, pw, ph = positions[parent_id]

        # Determine if this is a reminder or deadline boundary event
        is_reminder = 'rem' in be_id.lower()

        if is_reminder:
            bx = px + BE_REMINDER_DX
        else:
            bx = px + BE_DEADLINE_DX

        by = py + ph - BE_DY_FROM_BOTTOM
        positions[be_id] = (bx, by, BOUNDARY_W, BOUNDARY_H)

    lane_geometry = {
        'lane_y': lane_y,
        'lane_heights': lane_heights,
        'total_pool_height': total_pool_height,
        'pool_width': pool_width,
    }

    if verbose:
        print(f"  Layers: {max_layer + 1}")
        print(f"  Pool: {pool_width}x{total_pool_height}")
        for lid in effective_lanes:
            h = lane_heights[lid]
            y = lane_y[lid]
            cnt = sum(1 for n in positions if lane_map.get(n) == lid and n not in boundary_map)
            print(f"  Lane '{lid}': y={y}, h={h}, elements={cnt}")

    return positions, lane_geometry


# ─── Edge routing ────────────────────────────────────────────────────────────

def route_edges(flows, positions, elements, boundary_map, lane_map, lane_geometry, verbose=False):
    """Route edges orthogonally using compact Camunda Modeler style.
    Patterns (from reference):
      - Same lane, same Y: 2wp straight horizontal
      - Same lane, different Y: 3wp L-shape (vertical then horizontal)
      - Cross-lane forward: 3-4wp (right, down/up, right)
      - Backward: 3wp (right to far-right, up/down to target Y, left to target)
      - Boundary event: 2wp vertical or 3wp L-shape
    """
    routed = []
    all_rects = {}
    for eid, (x, y, w, h) in positions.items():
        all_rects[eid] = Rect(x, y, w, h)

    backward_idx = 0
    source_fan_out = defaultdict(int)
    target_fan_in = defaultdict(int)

    for flow in flows:
        src_id = flow.source_ref
        tgt_id = flow.target_ref

        if src_id not in positions or tgt_id not in positions:
            continue

        # Fan-out jitter
        fan_idx = max(source_fan_out[src_id], target_fan_in[tgt_id])
        source_fan_out[src_id] += 1
        target_fan_in[tgt_id] += 1
        fan_jitter = fan_idx * 15

        # Boundary event source: start from bottom center
        if src_id in boundary_map:
            waypoints = _route_from_boundary(src_id, tgt_id, positions, fan_jitter)
            waypoints = avoid_collisions(waypoints, src_id, tgt_id, boundary_map, all_rects)
            routed.append((flow.id, waypoints))
            continue

        sx, sy, sw, sh = positions[src_id]
        tx, ty, tw, th = positions[tgt_id]

        src_right = sx + sw
        src_cy = sy + sh / 2
        tgt_left = tx
        tgt_cy = ty + th / 2

        src_lane = lane_map.get(src_id, '')
        tgt_lane = lane_map.get(tgt_id, '')

        is_forward = tx > sx + sw - 10

        if is_forward:
            if src_lane == tgt_lane and abs(src_cy - tgt_cy) < 5:
                # Same lane, same Y → 2wp straight horizontal
                waypoints = [(src_right, src_cy), (tgt_left, tgt_cy)]
            elif src_lane == tgt_lane:
                # Same lane, different Y → 3wp L-shape
                # Go right from source, then vertical to target Y
                exit_x = src_right + 30 + fan_jitter
                waypoints = [
                    (src_right, src_cy),
                    (exit_x, src_cy),
                    (exit_x, tgt_cy),
                    (tgt_left, tgt_cy),
                ]
            else:
                # Cross-lane → 4wp Z-shape (compact mid-corridor)
                mid_x = src_right + 30 + fan_jitter
                waypoints = [
                    (src_right, src_cy),
                    (mid_x, src_cy),
                    (mid_x, tgt_cy),
                    (tgt_left, tgt_cy),
                ]
        else:
            # Backward flow → route above pool via corridor (avoids crossing elements)
            backward_idx += 1
            corridor_y = POOL_Y - 10 - (backward_idx * 12)
            waypoints = [
                (src_right, src_cy),
                (src_right + 20, src_cy),
                (src_right + 20, corridor_y),
                (tgt_left - 20, corridor_y),
                (tgt_left - 20, tgt_cy),
                (tgt_left, tgt_cy),
            ]

        waypoints = avoid_collisions(waypoints, src_id, tgt_id, boundary_map, all_rects)
        routed.append((flow.id, waypoints))

    if verbose:
        print(f"  Routed {len(routed)} edges")

    return routed


def _route_from_boundary(src_id, tgt_id, positions, fan_jitter):
    """Route edge from boundary event (bottom center) to target.
    Uses L-shape routing with offset to avoid crossing elements."""
    bx, by, bw, bh = positions[src_id]
    tx, ty, tw, th = positions[tgt_id]

    src_cx = bx + bw / 2
    src_bottom = by + bh
    tgt_left = tx
    tgt_cy = ty + th / 2

    # Offset the vertical corridor to the right of both source and target
    # to avoid crossing elements in between
    corridor_x = max(src_cx, tgt_left + tw) + 20 + fan_jitter

    # If target is in the same vertical area and close: 2wp vertical
    if abs(src_cx - (tgt_left + tw / 2)) < 20 and abs(by - ty) < 200:
        return [
            (src_cx, src_bottom),
            (src_cx, tgt_cy),
        ]

    # L-shape: down from boundary, then horizontal to target
    return [
        (src_cx, src_bottom),
        (src_cx, tgt_cy),
        (tgt_left, tgt_cy),
    ]


def avoid_collisions(waypoints, src_id, tgt_id, boundary_map, all_rects):
    """Collision avoidance: for each segment, check if it crosses a shape.
    For vertical segments: shift X to the right of the colliding shape.
    For horizontal segments: shift Y above/below the colliding shape.
    Single pass — no iterative detours to avoid explosion."""
    excluded = {src_id, tgt_id}
    if src_id in boundary_map:
        excluded.add(boundary_map[src_id])
    if tgt_id in boundary_map:
        excluded.add(boundary_map[tgt_id])

    collidable = {k: v for k, v in all_rects.items()
                  if k not in excluded
                  and not k.startswith('Lane_')
                  and not k.startswith('Pool_')
                  and not k.startswith('TextAnnotation_')}

    new_waypoints = [waypoints[0]]
    for seg_idx in range(len(waypoints) - 1):
        p1 = waypoints[seg_idx] if seg_idx == 0 else new_waypoints[-1]
        p2 = waypoints[seg_idx + 1]

        # Find first collision
        collider = None
        for eid, rect in collidable.items():
            shrunk = Rect(rect.x + 3, rect.y + 3, rect.width - 6, rect.height - 6)
            if shrunk.width > 0 and shrunk.height > 0 and line_intersects_rect(p1, p2, shrunk):
                collider = (eid, rect)
                break

        if collider:
            eid, rect = collider
            is_vertical = abs(p1[0] - p2[0]) < 2
            is_horizontal = abs(p1[1] - p2[1]) < 2

            if is_vertical:
                # Shift X to the right of colliding shape
                new_x = rect.right + 15
                new_waypoints.append((new_x, p1[1]))
                new_waypoints.append((new_x, p2[1]))
            elif is_horizontal:
                # Shift Y above or below colliding shape
                mid_y = (p1[1] + p2[1]) / 2
                if mid_y <= rect.center_y:
                    new_y = rect.y - 15
                else:
                    new_y = rect.bottom + 15
                new_waypoints.append((p1[0], new_y))
                new_waypoints.append((p2[0], new_y))
            else:
                # Diagonal — just keep it
                pass

        new_waypoints.append(p2)

    # Deduplicate consecutive identical or very close waypoints
    cleaned = [new_waypoints[0]]
    for wp in new_waypoints[1:]:
        if abs(wp[0] - cleaned[-1][0]) > 0.5 or abs(wp[1] - cleaned[-1][1]) > 0.5:
            cleaned.append(wp)
    return cleaned


# ─── DI Writer ───────────────────────────────────────────────────────────────

def write_di(root, model, positions, routed_edges, lane_geometry, verbose=False):
    """Remove old BPMNDiagram and write a new one."""
    bpmndi_ns = NS['bpmndi']
    dc_ns = NS['dc']
    di_ns = NS['di']

    # Register namespaces to preserve prefixes
    ET.register_namespace('bpmn', NS['bpmn'])
    ET.register_namespace('bpmndi', bpmndi_ns)
    ET.register_namespace('dc', dc_ns)
    ET.register_namespace('di', di_ns)
    ET.register_namespace('zeebe', 'http://camunda.org/schema/zeebe/1.0')
    ET.register_namespace('xsi', 'http://www.w3.org/2001/XMLSchema-instance')
    ET.register_namespace('modeler', 'http://camunda.org/schema/modeler/1.0')

    # Remove old diagram(s)
    for diag in root.findall(f'{{{bpmndi_ns}}}BPMNDiagram'):
        root.remove(diag)

    # Create new diagram
    diagram = ET.SubElement(root, f'{{{bpmndi_ns}}}BPMNDiagram', id='BPMNDiagram_1')

    # Determine plane bpmnElement
    plane_element = model['collab_id'] or model['process_id'] or 'Process_1'
    plane = ET.SubElement(diagram, f'{{{bpmndi_ns}}}BPMNPlane',
                          id='BPMNPlane_1', bpmnElement=plane_element)

    lane_y = lane_geometry['lane_y']
    lane_heights = lane_geometry['lane_heights']
    pool_width = lane_geometry['pool_width']
    total_pool_height = lane_geometry['total_pool_height']
    lane_order = model['lane_order']
    lane_width = pool_width - LANE_LABEL_WIDTH

    # Pool shape (if collaboration exists)
    if model['participant_id']:
        pool_shape = ET.SubElement(plane, f'{{{bpmndi_ns}}}BPMNShape',
                                   id=f"{model['participant_id']}_di",
                                   bpmnElement=model['participant_id'],
                                   isHorizontal='true')
        ET.SubElement(pool_shape, f'{{{dc_ns}}}Bounds',
                      x=str(POOL_X), y=str(POOL_Y),
                      width=str(int(pool_width)), height=str(int(total_pool_height)))
        ET.SubElement(pool_shape, f'{{{bpmndi_ns}}}BPMNLabel')

    # Lane shapes
    for lid in lane_order:
        ly = lane_y[lid]
        lh = lane_heights[lid]
        lane_shape = ET.SubElement(plane, f'{{{bpmndi_ns}}}BPMNShape',
                                   id=f'{lid}_di', bpmnElement=lid,
                                   isHorizontal='true')
        ET.SubElement(lane_shape, f'{{{dc_ns}}}Bounds',
                      x=str(LANE_X), y=str(int(ly)),
                      width=str(int(lane_width)), height=str(int(lh)))
        ET.SubElement(lane_shape, f'{{{bpmndi_ns}}}BPMNLabel')

    # Element shapes
    boundary_map = model['boundary_map']
    for eid, (x, y, w, h) in positions.items():
        if eid.startswith('Lane_') or eid.startswith('Pool_'):
            continue

        attrs = {'id': f'{eid}_di', 'bpmnElement': eid}

        # Gateway marker
        el = model['elements'].get(eid)
        if el and ('Gateway' in el.tag or 'gateway' in el.tag):
            attrs['isMarkerVisible'] = 'true'

        shape_el = ET.SubElement(plane, f'{{{bpmndi_ns}}}BPMNShape', **attrs)
        ET.SubElement(shape_el, f'{{{dc_ns}}}Bounds',
                      x=f'{x:.0f}', y=f'{y:.0f}',
                      width=f'{w:.0f}', height=f'{h:.0f}')
        ET.SubElement(shape_el, f'{{{bpmndi_ns}}}BPMNLabel')

    # Edge shapes
    for flow_id, waypoints in routed_edges:
        edge_el = ET.SubElement(plane, f'{{{bpmndi_ns}}}BPMNEdge',
                                id=f'{flow_id}_di', bpmnElement=flow_id)
        for wx, wy in waypoints:
            ET.SubElement(edge_el, f'{{{di_ns}}}waypoint',
                          x=f'{wx:.0f}', y=f'{wy:.0f}')

    if verbose:
        shapes_count = len([e for e in positions if not e.startswith('Lane_') and not e.startswith('Pool_')])
        print(f"  Wrote DI: {shapes_count} shapes, {len(routed_edges)} edges")


# ─── Main pipeline ───────────────────────────────────────────────────────────

def auto_layout(filepath, output=None, check_only=False, verbose=False):
    """Main auto-layout pipeline."""
    if verbose:
        print(f"=== BPMN Auto Layout ===")
        print(f"Input: {filepath}")
        print()

    # Parse
    tree = ET.parse(filepath)
    root = tree.getroot()
    model = parse_semantic_model(root)

    if verbose:
        print(f"[1/6] Parsed semantic model:")
        print(f"  Elements: {len(model['elements'])}")
        print(f"  Flows: {len(model['flows'])}")
        print(f"  Boundary events: {len(model['boundary_map'])}")
        print(f"  Lanes: {len(model['lane_order'])} {model['lane_order']}")
        print()

    if check_only:
        result = run_all_checks(filepath, verbose=verbose)
        return result['total']

    # Build graph
    adj, adj_rev, flow_elements = build_graph(
        model['elements'], model['flows'], model['boundary_map'])

    if verbose:
        print(f"[2/6] Built graph: {len(flow_elements)} nodes in flow")
        print()

    # Assign layers
    layers_dict = assign_layers(model['elements'], adj, adj_rev, model['boundary_map'])

    if verbose:
        max_l = max(layers_dict.values()) if layers_dict else 0
        print(f"[3/6] Assigned layers: {max_l + 1} layers")
        print()

    # Crossing minimization
    layers_by_num = barycenter_ordering(
        layers_dict, adj, model['lane_map'], model['lane_order'])

    if verbose:
        print(f"[4/6] Crossing minimization done")
        print()

    # Coordinate assignment
    if verbose:
        print(f"[5/6] Computing coordinates:")
    positions, lane_geometry = compute_coordinates(
        layers_by_num, model['elements'], model['boundary_map'],
        model['lane_map'], model['lane_order'], verbose=verbose)

    if verbose:
        print()

    # Edge routing
    if verbose:
        print(f"[6/6] Routing edges:")
    routed_edges = route_edges(
        model['flows'], positions, model['elements'],
        model['boundary_map'], model['lane_map'], lane_geometry, verbose=verbose)

    if verbose:
        print()

    # Write DI
    if verbose:
        print(f"Writing DI section:")
    write_di(root, model, positions, routed_edges, lane_geometry, verbose=verbose)

    # Save
    out_path = output or filepath
    tree.write(out_path, encoding='unicode', xml_declaration=True)

    # Post-process: ensure proper XML declaration
    _fix_xml_declaration(out_path)

    if verbose:
        print()
        print(f"Output: {out_path}")

    # Run checker on result
    if verbose:
        print()
        print(f"=== Post-layout check ===")
    result = run_all_checks(out_path, verbose=verbose)

    return result['total']


def _fix_xml_declaration(filepath):
    """Ensure the file starts with proper XML declaration."""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    if not content.startswith('<?xml'):
        content = '<?xml version="1.0" encoding="UTF-8"?>\n' + content

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)


# ─── CLI ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='BPMN Auto Layout — Sugiyama hierarchical layout for BPMN diagrams'
    )
    parser.add_argument('file', help='Path to BPMN file')
    parser.add_argument('-o', '--output', help='Output file (default: overwrite input)')
    parser.add_argument('--check', action='store_true', help='Only check layout, do not modify')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    args = parser.parse_args()

    issues = auto_layout(
        args.file,
        output=args.output,
        check_only=args.check,
        verbose=args.verbose,
    )

    sys.exit(0 if issues == 0 else 1)


if __name__ == '__main__':
    main()
