"""DAG execution: topological order over ``node.inputs``, one ``run`` per node.

The single kernel both front-ends share. The sort is *order-stable*: when several
nodes are ready, the one declared earliest runs first. That makes a linear
pipeline (sources, then transforms in given order, then sinks) execute in exactly
its declared order, so the kernel refactor is behavior-preserving for the CLI.
"""

from __future__ import annotations

from collections.abc import Sequence

from .engine import Engine
from .node import Node


def _topological_order(nodes: Sequence[Node]) -> list[Node]:
    by_id: dict[str, Node] = {}
    for node in nodes:
        assert node.id not in by_id, f"Duplicate node id {node.id!r}"
        by_id[node.id] = node

    known = set(by_id)
    # Only count dependencies that are themselves nodes; an input naming something
    # outside the graph is a validation error handled upstream (Graph.validate).
    remaining = {
        node.id: {dep for dep in node.inputs if dep in known} for node in nodes
    }

    ordered: list[Node] = []
    # Preserve declaration order among ready nodes (stable Kahn).
    while remaining:
        ready = [n.id for n in nodes if n.id in remaining and not remaining[n.id]]
        if not ready:
            raise ValueError(
                f"Cycle in graph among nodes: {sorted(remaining)}"
            )
        for node_id in ready:
            ordered.append(by_id[node_id])
            del remaining[node_id]
        for deps in remaining.values():
            deps.difference_update(ready)
    return ordered


def run_nodes(nodes: Sequence[Node], engine: Engine) -> None:
    for node in _topological_order(nodes):
        node.run(engine)
