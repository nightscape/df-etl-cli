"""Declarative processing graph (feature A): JSON/YAML -> validated DAG of nodes.

A graph file has two blocks::

    data:                       # sources (no inputs); key is the node id
      vmt:
        type: text
        path: data/vmt.csv
        infer: false
        empty: string
    pipeline:                   # nodes referencing each other by id
      - id: ops_paths
        type: sql
        inputs: [vmt]
        query: |
          SELECT issue_key, regexp_extract(description, '\\(([^)]+)\\)', 1) AS path
          FROM vmt
      - id: out
        type: parquet
        inputs: [ops_paths]
        path: out.parquet
        sink: true

Each node carries either an inline ``uri`` string *or* inline params (``path`` and
any other keys); exactly one. ``compile`` lowers every node to a kernel adapter
(``SourceNode``/``TransformNode``/``SinkNode``); ``run_nodes`` executes them.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from .node import Node, SinkNode, SourceNode, TransformNode
from .registry import source_sink_schemes
from .uri import ParsedUri

_RESERVED = frozenset({"id", "type", "inputs", "sink", "uri"})


@dataclass
class NodeSpec:
    id: str
    type: str
    inputs: list[str] = field(default_factory=list)
    uri: str | None = None
    params: dict[str, str] = field(default_factory=dict)
    sink: bool = False

    @classmethod
    def from_dict(cls, entry: dict, *, id: str, is_source: bool) -> "NodeSpec":
        assert "type" in entry, f"Node {id!r} is missing required 'type'"
        params = {k: v for k, v in entry.items() if k not in _RESERVED}
        uri = entry.get("uri")
        has_uri = uri is not None
        has_params = bool(params)
        assert has_uri != has_params, (
            f"Node {id!r} must set exactly one of 'uri' or inline params; "
            f"got uri={uri!r}, params={sorted(params)}"
        )
        inputs = [] if is_source else list(entry.get("inputs", []))
        assert not (is_source and entry.get("inputs")), (
            f"Source node {id!r} in 'data' cannot declare inputs"
        )
        return cls(
            id=id,
            type=entry["type"],
            inputs=inputs,
            uri=uri,
            params=params,
            sink=bool(entry.get("sink", False)),
        )

    def parsed_uri(self) -> ParsedUri:
        if self.uri is not None:
            return ParsedUri.parse(self.uri)
        return ParsedUri.from_params(self.type, self.params)


@dataclass
class Graph:
    sources: dict[str, NodeSpec]
    pipeline: list[NodeSpec]

    @classmethod
    def load(cls, path: str) -> "Graph":
        suffix = Path(path).suffix.lower()
        text = Path(path).read_text()
        if suffix == ".json":
            doc = json.loads(text)
        elif suffix in (".yaml", ".yml"):
            doc = yaml.safe_load(text)
        else:
            raise ValueError(
                f"Cannot load graph {path!r}: extension {suffix!r} not one of "
                ".json/.yaml/.yml"
            )
        return cls.from_dict(doc)

    @classmethod
    def from_dict(cls, doc: dict) -> "Graph":
        data = doc.get("data", {})
        pipeline = doc.get("pipeline", [])
        sources = {
            sid: NodeSpec.from_dict(entry, id=sid, is_source=True)
            for sid, entry in data.items()
        }
        steps = []
        for entry in pipeline:
            assert "id" in entry, f"Pipeline node is missing required 'id': {entry}"
            steps.append(NodeSpec.from_dict(entry, id=entry["id"], is_source=False))
        return cls(sources=sources, pipeline=steps)

    def _all_specs(self) -> list[NodeSpec]:
        return list(self.sources.values()) + self.pipeline

    def validate(self) -> None:
        specs = self._all_specs()
        # (a) ids unique across data + pipeline
        seen: set[str] = set()
        for spec in specs:
            assert spec.id not in seen, f"Duplicate node id {spec.id!r}"
            seen.add(spec.id)
        # (b) membership — every input references a known id (order-independent)
        for spec in specs:
            for dep in spec.inputs:
                assert dep in seen, (
                    f"Node {spec.id!r} input {dep!r} is not a declared id; "
                    f"known: {sorted(seen)}"
                )
        # (c) acyclicity (independent of declaration order)
        self._assert_acyclic(specs)
        # (d) sink/transform input arity
        for spec in self.pipeline:
            if spec.sink:
                assert len(spec.inputs) == 1, (
                    f"Sink node {spec.id!r} must have exactly one input, got {spec.inputs}"
                )
            else:
                assert spec.inputs, f"Transform node {spec.id!r} has no input"

    @staticmethod
    def _assert_acyclic(specs: list[NodeSpec]) -> None:
        deps = {spec.id: set(spec.inputs) for spec in specs}
        resolved: set[str] = set()
        while len(resolved) < len(deps):
            ready = [nid for nid, d in deps.items() if nid not in resolved and d <= resolved]
            if not ready:
                pending = sorted(set(deps) - resolved)
                raise ValueError(f"Cycle in graph among nodes: {pending}")
            resolved.update(ready)

    def compile(self) -> list[Node]:
        self.validate()
        scheme_is_source = set(source_sink_schemes())
        nodes: list[Node] = []
        for spec in self.sources.values():
            nodes.append(SourceNode(id=spec.id, uri=spec.parsed_uri()))
        for spec in self.pipeline:
            uri = spec.parsed_uri()
            if spec.sink:
                nodes.append(SinkNode(id=spec.id, inputs=spec.inputs, uri=uri))
            elif not spec.inputs:
                assert spec.type in scheme_is_source, (
                    f"Node {spec.id!r} has no inputs but type {spec.type!r} is not "
                    f"a source scheme"
                )
                nodes.append(SourceNode(id=spec.id, uri=uri, inputs=[]))
            else:
                nodes.append(TransformNode(id=spec.id, inputs=spec.inputs, uri=uri))
        return nodes
