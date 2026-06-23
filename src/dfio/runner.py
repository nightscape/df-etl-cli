"""DAG execution: topological order over ``node.inputs``, one ``run`` per node.

The single kernel both front-ends share. The sort is *order-stable*: when several
nodes are ready, the one declared earliest runs first. That makes a linear
pipeline (sources, then transforms in given order, then sinks) execute in exactly
its declared order, so the kernel refactor is behavior-preserving for the CLI.
"""

from __future__ import annotations

import dataclasses
import os
from collections.abc import Sequence

import ibis

from .engine import Engine
from .node import Node, PluginNode, SinkNode


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


def _cache_path(out_dir: str, node_id: str) -> str:
    return os.path.join(out_dir, f"{node_id}.parquet")


def _resolve_sink_paths(nodes: Sequence[Node], out_dir: str | None) -> None:
    """Relative kernel-sink paths resolve under ``out_dir`` (feature J):
    ``coverage.csv`` -> ``out_dir/coverage.csv``. Plugin nodes write their own
    files via ``ctx.out_dir`` and are left untouched."""
    if out_dir is None:
        return
    for node in nodes:
        if isinstance(node, SinkNode) and node.uri.path and not os.path.isabs(node.uri.path):
            resolved = os.path.join(out_dir, node.uri.path)
            node.uri = dataclasses.replace(node.uri, path=resolved)


def run_graph(
    graph,
    engine: Engine,
    *,
    out_dir: str | None = None,
    only: Sequence[str] | None = None,
    runtime: object = None,
) -> None:
    """Compile ``graph`` and run it in topological order over the catalog.

    ``runtime`` is passed opaquely into every ``PluginNode``'s ``NodeContext``.
    ``only`` runs just those node ids: any input produced by a node *not* in
    ``only`` is loaded from its ``out_dir/<id>.parquet`` cache (raising if absent)
    rather than recomputed. ``cache: true`` nodes persist their output to that
    cache after running. Relative sink paths resolve under ``out_dir``.
    """
    if out_dir is not None:
        os.makedirs(out_dir, exist_ok=True)
    nodes = graph.compile()
    _resolve_sink_paths(nodes, out_dir)
    ordered = _topological_order(nodes)
    cache_flags = {spec.id: spec.cache for spec in graph._all_specs()}
    run_ids = set(only) if only is not None else {node.id for node in ordered}

    for node in ordered:
        if node.id not in run_ids:
            continue
        for dep in node.inputs:
            if dep not in run_ids and not engine.has(dep):
                _load_cache(engine, dep, out_dir)
        if isinstance(node, PluginNode):
            node.runtime = runtime
            node.out_dir = out_dir
        node.run(engine)
        if cache_flags.get(node.id) and engine.has(node.id):
            assert out_dir is not None, (
                f"Node {node.id!r} has cache:true but no out_dir was given"
            )
            os.makedirs(out_dir, exist_ok=True)
            engine.table(node.id).to_polars().write_parquet(_cache_path(out_dir, node.id))


def _load_cache(engine: Engine, node_id: str, out_dir: str | None) -> None:
    import polars as pl

    assert out_dir is not None, (
        f"Input {node_id!r} is not being recomputed and no out_dir holds its cache; "
        f"run {node_id!r} first"
    )
    path = _cache_path(out_dir, node_id)
    assert os.path.exists(path), (
        f"Input {node_id!r} has no cache at {path!r}; run {node_id!r} first"
    )
    engine.register(node_id, ibis.memtable(pl.read_parquet(path)))
