"""Pipeline orchestration. Ports ETL.scala (Source / Transformation / Sink / run).

URIs are parsed into named pipeline steps. Sources read and register a named
table; transformations read a named input and register a named output; sinks
read a named table and write it. Names follow the Scala convention: a missing
source name defaults to ``"source"``, a missing sink name to ``"sink"``, and
hyphens become underscores so names are valid SQL identifiers.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import ibis

from .engine import Engine
from .registry import as_sink, as_source, build_source_sink, build_transformer
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

    def run(self, engine: Engine) -> ibis.Table:
        source = as_source(build_source_sink(self.uri, engine))
        return engine.register(self.name, source.read())


@dataclass
class Sink:
    name: str
    uri: ParsedUri

    @classmethod
    def parse(cls, uri_str: str) -> "Sink":
        uri = ParsedUri.parse(uri_str)
        _, name = uri.scheme_and_name()
        return cls(_normalize(name) if name else DEFAULT_SINK, uri)

    def run(self, engine: Engine) -> bool:
        sink = as_sink(build_source_sink(self.uri, engine))
        return sink.write(engine.table(self.name))


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

    def run(self, engine: Engine) -> ibis.Table:
        transformer = build_transformer(self.uri)
        output = transformer(engine.table(self.input_name))
        return engine.register(self.output_name, output)


@dataclass
class ETL:
    sources: list[Source]
    sinks: list[Sink]
    transforms: list[Transformation] = field(default_factory=list)
    backend: str = "duckdb"

    def run(self, engine: Engine | None = None) -> None:
        engine = engine or Engine.from_config(self.backend)
        for source in self.sources:
            source.run(engine)
        transforms = self.transforms or [
            Transformation(DEFAULT_SOURCE, DEFAULT_SINK, ParsedUri.parse("identity:///"))
        ]
        for transform in transforms:
            transform.run(engine)
        for sink in self.sinks:
            assert sink.run(engine), f"Writing to sink {sink.name!r} failed"
        print("Write successful")
