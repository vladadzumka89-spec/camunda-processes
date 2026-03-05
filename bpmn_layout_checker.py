#!/usr/bin/env python3
"""
BPMN Layout Checker — analyzes BPMNDI section for cosmetic layout issues.
Checks:
1. Overlapping shapes (bounding rectangles overlap)
2. Overlapping edges (shared waypoints or crossing through shapes)
3. Boundary events positioned on parent task border
4. Elements within their designated lane boundaries
5. Edge-shape collision detection (edge segments crossing through non-source/target shapes)
"""

import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Optional
import argparse
import math
import sys

# Namespaces
NS = {
    'bpmn': 'http://www.omg.org/spec/BPMN/20100524/MODEL',
    'bpmndi': 'http://www.omg.org/spec/BPMN/20100524/DI',
    'dc': 'http://www.omg.org/spec/DD/20100524/DC',
    'di': 'http://www.omg.org/spec/DD/20100524/DI',
}

@dataclass
class Rect:
    x: float
    y: float
    width: float
    height: float

    @property
    def right(self):
        return self.x + self.width

    @property
    def bottom(self):
        return self.y + self.height

    @property
    def center_x(self):
        return self.x + self.width / 2

    @property
    def center_y(self):
        return self.y + self.height / 2

    def overlaps(self, other: 'Rect') -> bool:
        return not (self.right <= other.x or other.right <= self.x or
                    self.bottom <= other.y or other.bottom <= self.y)

    def overlap_area(self, other: 'Rect') -> float:
        ox = max(0, min(self.right, other.right) - max(self.x, other.x))
        oy = max(0, min(self.bottom, other.bottom) - max(self.y, other.y))
        return ox * oy

    def contains_point(self, px: float, py: float) -> bool:
        return self.x <= px <= self.right and self.y <= py <= self.bottom

    def __repr__(self):
        return f"Rect(x={self.x}, y={self.y}, w={self.width}, h={self.height})"


@dataclass
class Shape:
    id: str
    bpmn_element: str
    bounds: Rect
    is_boundary_event: bool = False
    attached_to: Optional[str] = None


@dataclass
class Edge:
    id: str
    bpmn_element: str
    waypoints: list  # list of (x, y) tuples
    source_ref: Optional[str] = None
    target_ref: Optional[str] = None


def parse_bpmn(filepath):
    tree = ET.parse(filepath)
    root = tree.getroot()

    shapes = {}
    edges = []
    boundary_events = {}  # id -> attachedToRef
    lane_assignments = {}  # from the BPMN process section
    lane_shapes = {}  # lane_id -> Rect (from BPMNDI)

    # Parse boundary events from process model
    for be in root.iter(f"{{{NS['bpmn']}}}boundaryEvent"):
        be_id = be.get('id')
        attached = be.get('attachedToRef')
        if be_id and attached:
            boundary_events[be_id] = attached

    # Parse lane assignments from process model
    for lane in root.iter(f"{{{NS['bpmn']}}}lane"):
        lane_id = lane.get('id')
        for ref in lane.findall(f"{{{NS['bpmn']}}}flowNodeRef"):
            if ref.text:
                lane_assignments[ref.text.strip()] = lane_id

    # Parse sequence flows for source/target refs
    seq_flow_refs = {}
    for sf in root.iter(f"{{{NS['bpmn']}}}sequenceFlow"):
        sf_id = sf.get('id')
        src = sf.get('sourceRef')
        tgt = sf.get('targetRef')
        if sf_id:
            seq_flow_refs[sf_id] = (src, tgt)

    # Parse BPMNDI shapes
    diagram = root.find(f".//{{{NS['bpmndi']}}}BPMNDiagram")
    plane = diagram.find(f"{{{NS['bpmndi']}}}BPMNPlane")

    for shape_el in plane.findall(f"{{{NS['bpmndi']}}}BPMNShape"):
        shape_id = shape_el.get('id')
        bpmn_el = shape_el.get('bpmnElement')
        bounds_el = shape_el.find(f"{{{NS['dc']}}}Bounds")
        if bounds_el is not None:
            bounds = Rect(
                x=float(bounds_el.get('x')),
                y=float(bounds_el.get('y')),
                width=float(bounds_el.get('width')),
                height=float(bounds_el.get('height'))
            )
            is_be = bpmn_el in boundary_events
            attached = boundary_events.get(bpmn_el)
            s = Shape(id=shape_id, bpmn_element=bpmn_el, bounds=bounds,
                      is_boundary_event=is_be, attached_to=attached)
            shapes[bpmn_el] = s

            # Track lane shapes
            if bpmn_el.startswith('Lane_'):
                lane_shapes[bpmn_el] = bounds

    # Parse BPMNDI edges
    for edge_el in plane.findall(f"{{{NS['bpmndi']}}}BPMNEdge"):
        edge_id = edge_el.get('id')
        bpmn_el = edge_el.get('bpmnElement')
        wps = []
        for wp in edge_el.findall(f"{{{NS['di']}}}waypoint"):
            wps.append((float(wp.get('x')), float(wp.get('y'))))
        src_ref, tgt_ref = seq_flow_refs.get(bpmn_el, (None, None))
        edges.append(Edge(id=edge_id, bpmn_element=bpmn_el, waypoints=wps,
                          source_ref=src_ref, target_ref=tgt_ref))

    return shapes, edges, boundary_events, lane_assignments, lane_shapes


# ─── Check 1: Overlapping Shapes ───

def check_overlapping_shapes(shapes):
    issues = []
    # Exclude lanes and pool from overlap check
    exclude_prefixes = ('Lane_', 'Pool_', 'TextAnnotation_')
    elements = [(k, v) for k, v in shapes.items()
                if not any(k.startswith(p) for p in exclude_prefixes)]

    # Also identify boundary events to allow overlap with their parent
    for i in range(len(elements)):
        for j in range(i + 1, len(elements)):
            id_a, shape_a = elements[i]
            id_b, shape_b = elements[j]

            # Boundary events are expected to overlap with their parent
            if shape_a.is_boundary_event and shape_a.attached_to == id_b:
                continue
            if shape_b.is_boundary_event and shape_b.attached_to == id_a:
                continue

            if shape_a.bounds.overlaps(shape_b.bounds):
                area = shape_a.bounds.overlap_area(shape_b.bounds)
                issues.append(
                    f"OVERLAP: '{id_a}' {shape_a.bounds} overlaps with "
                    f"'{id_b}' {shape_b.bounds} "
                    f"(overlap area: {area:.0f} sq px)"
                )
    return issues


# ─── Check 2: Overlapping / Shared Waypoint Edges ───

def check_overlapping_edges(edges):
    issues = []
    for i in range(len(edges)):
        for j in range(i + 1, len(edges)):
            e1 = edges[i]
            e2 = edges[j]
            shared = set(e1.waypoints) & set(e2.waypoints)
            # Filter out trivial: two edges can share a start/end at a gateway etc.
            # Only flag if they share >1 waypoint (indicates co-linear segments)
            if len(shared) > 1:
                issues.append(
                    f"SHARED WAYPOINTS: Edge '{e1.bpmn_element}' and '{e2.bpmn_element}' "
                    f"share {len(shared)} waypoints: {sorted(shared)}"
                )
    return issues


# ─── Check 3: Boundary Events on Parent Border ───

def check_boundary_events(shapes, boundary_events):
    issues = []
    TOLERANCE = 5  # pixels

    for be_id, parent_id in boundary_events.items():
        if be_id not in shapes or parent_id not in shapes:
            issues.append(f"MISSING SHAPE: Boundary event '{be_id}' or parent '{parent_id}' not found in diagram")
            continue

        be_shape = shapes[be_id]
        parent_shape = shapes[parent_id]
        be_bounds = be_shape.bounds
        parent_bounds = parent_shape.bounds

        # Center of boundary event
        be_cx = be_bounds.center_x
        be_cy = be_bounds.center_y

        # Check if boundary event center is near any edge of the parent
        on_left = abs(be_cx - parent_bounds.x) <= TOLERANCE
        on_right = abs(be_cx - parent_bounds.right) <= TOLERANCE
        on_top = abs(be_cy - parent_bounds.y) <= TOLERANCE
        on_bottom = abs(be_cy - parent_bounds.bottom) <= TOLERANCE

        # For horizontal edges, x should be within parent x range
        x_in_range = (parent_bounds.x - be_bounds.width/2 - TOLERANCE <= be_cx <=
                       parent_bounds.right + be_bounds.width/2 + TOLERANCE)
        # For vertical edges, y should be within parent y range
        y_in_range = (parent_bounds.y - be_bounds.height/2 - TOLERANCE <= be_cy <=
                       parent_bounds.bottom + be_bounds.height/2 + TOLERANCE)

        on_border = (
            (on_left and y_in_range) or
            (on_right and y_in_range) or
            (on_top and x_in_range) or
            (on_bottom and x_in_range)
        )

        if not on_border:
            # Calculate distances to each border
            dist_left = abs(be_cx - parent_bounds.x)
            dist_right = abs(be_cx - parent_bounds.right)
            dist_top = abs(be_cy - parent_bounds.y)
            dist_bottom = abs(be_cy - parent_bounds.bottom)
            min_dist = min(dist_left, dist_right, dist_top, dist_bottom)

            issues.append(
                f"BOUNDARY OFF-BORDER: '{be_id}' (center: {be_cx:.0f},{be_cy:.0f}) "
                f"is NOT on the border of parent '{parent_id}' "
                f"(bounds: x={parent_bounds.x:.0f},y={parent_bounds.y:.0f} "
                f"to x={parent_bounds.right:.0f},y={parent_bounds.bottom:.0f}). "
                f"Min distance to nearest border: {min_dist:.1f}px"
            )

    return issues


# ─── Check 4: Elements Within Lane Boundaries ───

def check_lane_membership(shapes, lane_assignments, lane_shapes):
    """Check that each element assigned to a lane has its shape within that lane's bounds.
    Uses dynamic lane_assignments parsed from BPMN XML <bpmn:lane>/<bpmn:flowNodeRef>."""
    issues = []

    # Build reverse mapping: lane_id -> [element_ids] from parsed lane_assignments
    lanes_to_elements = {}
    for elem_id, lane_id in lane_assignments.items():
        lanes_to_elements.setdefault(lane_id, []).append(elem_id)

    for lane_id, element_ids in lanes_to_elements.items():
        if lane_id not in lane_shapes:
            issues.append(f"MISSING LANE SHAPE: Lane '{lane_id}' not found in diagram")
            continue

        lane_bounds = lane_shapes[lane_id]

        for elem_id in element_ids:
            if elem_id not in shapes:
                issues.append(f"MISSING ELEMENT: '{elem_id}' (assigned to '{lane_id}') not found in diagram shapes")
                continue

            elem_bounds = shapes[elem_id].bounds
            # Check if the element's bounding box is fully within the lane
            within = (elem_bounds.x >= lane_bounds.x - 1 and
                      elem_bounds.y >= lane_bounds.y - 1 and
                      elem_bounds.right <= lane_bounds.right + 1 and
                      elem_bounds.bottom <= lane_bounds.bottom + 1)

            if not within:
                # Calculate how far outside the lane it is
                overflows = []
                if elem_bounds.x < lane_bounds.x:
                    overflows.append(f"left by {lane_bounds.x - elem_bounds.x:.0f}px")
                if elem_bounds.y < lane_bounds.y:
                    overflows.append(f"top by {lane_bounds.y - elem_bounds.y:.0f}px")
                if elem_bounds.right > lane_bounds.right:
                    overflows.append(f"right by {elem_bounds.right - lane_bounds.right:.0f}px")
                if elem_bounds.bottom > lane_bounds.bottom:
                    overflows.append(f"bottom by {elem_bounds.bottom - lane_bounds.bottom:.0f}px")

                issues.append(
                    f"OUT OF LANE: '{elem_id}' {elem_bounds} is outside its lane "
                    f"'{lane_id}' {lane_bounds}. Overflows: {', '.join(overflows)}"
                )

    return issues


# ─── Check 5: Edge-Shape Collision Detection ───

def line_intersects_rect(p1, p2, rect):
    """Check if line segment from p1 to p2 passes through the rectangle.
    Uses parametric line-rectangle intersection."""
    x1, y1 = p1
    x2, y2 = p2
    dx = x2 - x1
    dy = y2 - y1

    # Check if either endpoint is inside the rect
    if rect.contains_point(x1, y1) or rect.contains_point(x2, y2):
        return True

    # Use Liang-Barsky algorithm
    p = [-dx, dx, -dy, dy]
    q = [x1 - rect.x, rect.right - x1, y1 - rect.y, rect.bottom - y1]

    t_min = 0.0
    t_max = 1.0

    for i in range(4):
        if abs(p[i]) < 1e-10:
            if q[i] < 0:
                return False
        else:
            t = q[i] / p[i]
            if p[i] < 0:
                t_min = max(t_min, t)
            else:
                t_max = min(t_max, t)

    return t_min <= t_max


def check_edge_shape_collisions(shapes, edges):
    """Check if edge waypoint segments cross through shapes that are NOT
    the source or target of that edge."""
    issues = []

    # Elements to check collisions against (exclude lanes, pool, text annotations, boundary events)
    exclude_prefixes = ('Lane_', 'Pool_', 'TextAnnotation_')
    collidable = {k: v for k, v in shapes.items()
                  if not any(k.startswith(p) for p in exclude_prefixes)
                  and not v.is_boundary_event}

    for edge in edges:
        if len(edge.waypoints) < 2:
            continue

        src = edge.source_ref
        tgt = edge.target_ref

        # For boundary event flows, the source might be a boundary event
        # which is attached to a parent — we should also exclude the parent
        excluded_elements = {src, tgt}
        if src in shapes and shapes[src].is_boundary_event:
            excluded_elements.add(shapes[src].attached_to)
        if tgt in shapes and shapes[tgt].is_boundary_event:
            excluded_elements.add(shapes[tgt].attached_to)

        for seg_idx in range(len(edge.waypoints) - 1):
            p1 = edge.waypoints[seg_idx]
            p2 = edge.waypoints[seg_idx + 1]

            for elem_id, elem_shape in collidable.items():
                if elem_id in excluded_elements:
                    continue

                if line_intersects_rect(p1, p2, elem_shape.bounds):
                    # Verify this is a real crossing and not just touching an edge
                    # by checking with a slightly shrunk rectangle (2px margin)
                    shrunk = Rect(
                        elem_shape.bounds.x + 2,
                        elem_shape.bounds.y + 2,
                        elem_shape.bounds.width - 4,
                        elem_shape.bounds.height - 4
                    )
                    if shrunk.width > 0 and shrunk.height > 0 and line_intersects_rect(p1, p2, shrunk):
                        issues.append(
                            f"EDGE COLLISION: Edge '{edge.bpmn_element}' "
                            f"(segment {seg_idx}: ({p1[0]:.0f},{p1[1]:.0f})->({p2[0]:.0f},{p2[1]:.0f})) "
                            f"crosses through shape '{elem_id}' {elem_shape.bounds} "
                            f"(source='{src}', target='{tgt}')"
                        )

    return issues


# ─── Run all checks ───

def run_all_checks(filepath, verbose=True):
    """Run all layout checks on a BPMN file. Returns dict with issues per category and total count."""
    shapes, edges, boundary_events, lane_assignments, lane_shapes = parse_bpmn(filepath)

    if verbose:
        print(f"Analyzing BPMN layout: {filepath}")
        print("=" * 100)
        print(f"\nParsed {len(shapes)} shapes, {len(edges)} edges, {len(boundary_events)} boundary events")
        print(f"Lane shapes: {list(lane_shapes.keys())}")
        print()

    checks = [
        ("OVERLAPPING SHAPES", check_overlapping_shapes(shapes)),
        ("OVERLAPPING/SHARED EDGES", check_overlapping_edges(edges)),
        ("BOUNDARY EVENTS ON PARENT BORDER", check_boundary_events(shapes, boundary_events)),
        ("ELEMENTS WITHIN LANE BOUNDARIES", check_lane_membership(shapes, lane_assignments, lane_shapes)),
        ("EDGE-SHAPE COLLISION DETECTION", check_edge_shape_collisions(shapes, edges)),
    ]

    results = {}
    total = 0
    for idx, (name, issues) in enumerate(checks, 1):
        results[name] = issues
        total += len(issues)
        if verbose:
            print("=" * 100)
            print(f"CHECK {idx}: {name}")
            print("=" * 100)
            if issues:
                for issue in issues:
                    print(f"  [!] {issue}")
            else:
                print(f"  [OK] No issues found")
            print()

    if verbose:
        print("=" * 100)
        print(f"SUMMARY: {total} total issues found")
        for name, issues in results.items():
            print(f"  - {name}: {len(issues)}")
        print("=" * 100)

    return {"checks": results, "total": total}


# ─── Main ───

def main():
    parser = argparse.ArgumentParser(
        description="BPMN Layout Checker — analyzes BPMNDI for cosmetic layout issues"
    )
    parser.add_argument("file", nargs="?", help="Path to BPMN file to analyze")
    parser.add_argument("-q", "--quiet", action="store_true", help="Only print summary line")
    args = parser.parse_args()

    if not args.file:
        parser.print_help()
        sys.exit(1)

    result = run_all_checks(args.file, verbose=not args.quiet)

    if args.quiet:
        print(f"{result['total']} issues found in {args.file}")

    sys.exit(0 if result["total"] == 0 else 1)


if __name__ == '__main__':
    main()
