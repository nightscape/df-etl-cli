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

from .node import Node, PluginNode, SinkNode, SourceNode, TransformNode
from .registry import build_node, node_names, source_sink_schemes
from .uri import ParsedUri

_RESERVED = frozenset({"id", "type", "inputs", "sink", "uri", "cache"})


def _scalar_str(value):
    """Coerce an inline YAML param value to the string a scheme parser expects.

    Query-string params always arrive as strings; inline params keep their native
    YAML type (``infer: false`` is a ``bool``, ``topn: 50`` an ``int``). Parsers
    like ``text``'s do ``qp.get("infer").lower()``, so a non-string scalar crashes.
    Coerce scalars to the same text the URI path would carry (``True`` -> ``"true"``,
    ``50`` -> ``"50"``); leave structured values (dict/list) for parsers that take
    them verbatim.
    """
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return value
    if value is None or isinstance(value, (int, float)):
        return json.dumps(value)
    return value


@dataclass
class NodeSpec:
    id: str
    type: str
    inputs: list[str] = field(default_factory=list)
    uri: str | None = None
    params: dict[str, str] = field(default_factory=dict)
    sink: bool = False
    cache: bool = False

    @classmethod
    def from_dict(cls, entry: dict, *, id: str, is_source: bool) -> "NodeSpec":
        assert "type" in entry, f"Node {id!r} is missing required 'type'"
        params = {
            k: _scalar_str(v) for k, v in entry.items() if k not in _RESERVED
        }
        uri = entry.get("uri")
        assert not (uri is not None and params), (
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
            cache=bool(entry.get("cache", False)),
        )

    def assert_single_config(self) -> None:
        """Kernel (non-plugin) nodes need exactly one of ``uri`` / inline params."""
        has_uri = self.uri is not None
        has_params = bool(self.params)
        assert has_uri != has_params, (
            f"Node {self.id!r} must set exactly one of 'uri' or inline params; "
            f"got uri={self.uri!r}, params={sorted(self.params)}"
        )

    def parsed_uri(self) -> ParsedUri:
        if self.uri is not None:
            return ParsedUri.parse(self.uri)
        return ParsedUri.from_params(self.type, self.params)


@dataclass
class Graph:
    sources: dict[str, NodeSpec]
    pipeline: list[NodeSpec]
    engine: str = "duckdb"

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
        return cls(sources=sources, pipeline=steps, engine=doc.get("engine", "duckdb"))

    def bind(self, name: str, path: str) -> None:
        """Rebind a declared source's ``path`` param (feature I) without editing
        the file. The graph file stays stable; the CLI/`coverage run vmt=NEW.csv`
        drives a fresh input through here."""
        assert name in self.sources, (
            f"Cannot bind unknown source {name!r}; known: {sorted(self.sources)}"
        )
        self.sources[name].params["path"] = path

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
        # (d) per-kind config + arity. Plugin nodes (a registered ``dfio.nodes``
        # type) are exempt: they take N inputs, may be pure sinks, and may carry
        # zero params — so only kernel sources/transforms/sinks are checked here.
        plugins = set(node_names())
        for spec in self.sources.values():
            spec.assert_single_config()
        for spec in self.pipeline:
            if spec.type in plugins:
                continue
            spec.assert_single_config()
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
        plugins = set(node_names())
        nodes: list[Node] = []
        for spec in self.sources.values():
            nodes.append(SourceNode(id=spec.id, uri=spec.parsed_uri()))
        for spec in self.pipeline:
            if spec.type in plugins:
                nodes.append(
                    PluginNode(
                        id=spec.id,
                        inputs=spec.inputs,
                        fn=build_node(spec.type),
                        params=spec.params,
                        cache=spec.cache,
                    )
                )
                continue
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
