"""Pipeline orchestration. Ports ETL.scala (Source / Transformation / Sink / run).

URIs are parsed into named pipeline steps. Sources read and register a named
table; transformations read a named input and register a named output; sinks
read a named table and write it. Names follow the Scala convention: a missing
source name defaults to ``"source"``, a missing sink name to ``"sink"``, and
hyphens become underscores so names are valid SQL identifiers.

The linear pipeline is *the same kernel* as the declarative graph: ``ETL.run``
compiles its steps to ``Node`` adapters and hands them to ``run_nodes``. The
per-step ``run`` methods are thin delegators kept for the direct-call public API.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import ibis

from .engine import Engine
from .node import SinkNode, SourceNode, TransformNode
from .runner import run_nodes
from .uri import ParsedUri

DEFAULT_SOURCE = "source"
DEFAULT_SINK = "sink"


def _normalize(name: str) -> str:
    return name.replace("-", "_")


@dataclass
class Source:
    name: str
    uri: ParsedUri

    @classmethod
    def parse(cls, uri_str: str) -> "Source":
        uri = ParsedUri.parse(uri_str)
        _, name = uri.scheme_and_name()
        return cls(_normalize(name) if name else DEFAULT_SOURCE, uri)

    def to_node(self) -> SourceNode:
        return SourceNode(id=self.name, uri=self.uri)

    def run(self, engine: Engine) -> ibis.Table:
        self.to_node().run(engine)
        return engine.table(self.name)


@dataclass
class Sink:
    name: str
    uri: ParsedUri

    @classmethod
    def parse(cls, uri_str: str) -> "Sink":
        uri = ParsedUri.parse(uri_str)
        _, name = uri.scheme_and_name()
        return cls(_normalize(name) if name else DEFAULT_SINK, uri)

    def to_node(self, index: int = 0) -> SinkNode:
        # Sinks register nothing, so their node id only labels the step; it is
        # kept distinct from the table they read (``inputs=[self.name]``) so it
        # never collides with the producing node's id.
        return SinkNode(id=f"__sink_{index}_{self.name}", inputs=[self.name], uri=self.uri)

    def run(self, engine: Engine) -> bool:
        self.to_node().run(engine)
        return True


@dataclass
class Transformation:
    input_name: str
    output_name: str
    uri: ParsedUri

    @classmethod
    def parse(cls, uri_str: str) -> "Transformation":
        uri = ParsedUri.parse(uri_str)
        _, src, sink = uri.scheme_source_sink()
        return cls(
            _normalize(src) if src else DEFAULT_SOURCE,
            _normalize(sink) if sink else DEFAULT_SINK,
            uri,
        )

    def to_node(self) -> TransformNode:
        return TransformNode(id=self.output_name, inputs=[self.input_name], uri=self.uri)

    def run(self, engine: Engine) -> ibis.Table:
        self.to_node().run(engine)
        return engine.table(self.output_name)


@dataclass
class ETL:
    sources: list[Source]
    sinks: list[Sink]
    transforms: list[Transformation] = field(default_factory=list)
    backend: str = "duckdb"

    def run(self, engine: Engine | None = None) -> None:
        engine = engine or Engine.from_config(self.backend)
        transforms = self.transforms or [
            Transformation(DEFAULT_SOURCE, DEFAULT_SINK, ParsedUri.parse("identity:///"))
        ]
        nodes = (
            [source.to_node() for source in self.sources]
            + [transform.to_node() for transform in transforms]
            + [sink.to_node(i) for i, sink in enumerate(self.sinks)]
        )
        run_nodes(nodes, engine)
        print("Write successful")
