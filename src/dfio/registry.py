"""Scheme dispatch over built-in and plugin parsers.

Ports ``DataFrameUrlParser`` / ``TransformerUrlParser``. Built-ins are listed
explicitly; external packages add schemes via the ``dfio.sources`` /
``dfio.transforms`` entry points (the Python analogue of Java ServiceLoader).
"""

from __future__ import annotations

from functools import cache
from importlib.metadata import entry_points

from .base import DataFrameSink, DataFrameSource, TransformerParser, UriParser
from .engine import Engine
from .uri import ParsedUri


def _builtin_uri_parsers() -> list[UriParser]:
    from .sources.console import ConsoleUriParser
    from .sources.parquet import ParquetUriParser
    from .sources.text import TextUriParser
    from .sources.values import ValuesUriParser

    return [ConsoleUriParser(), ValuesUriParser(), TextUriParser(), ParquetUriParser()]


def _builtin_transformer_parsers() -> list[TransformerParser]:
    from .transforms.flatten import FlattenAndExplodeParser, FlattenParser
    from .transforms.identity import IdentityParser
    from .transforms.sql import SqlParser

    return [IdentityParser(), SqlParser(), FlattenParser(), FlattenAndExplodeParser()]


def _load_plugins(group: str) -> list:
    return [ep.load()() for ep in entry_points(group=group)]


@cache
def uri_parsers() -> list[UriParser]:
    return _builtin_uri_parsers() + _load_plugins("dfio.sources")


@cache
def transformer_parsers() -> list[TransformerParser]:
    return _builtin_transformer_parsers() + _load_plugins("dfio.transforms")


@cache
def node_factories() -> dict:
    """Domain-node plugins (feature F): ``dfio.nodes`` entry points each resolve to
    a ``(NodeContext) -> None`` callable, keyed by the node ``type`` in the graph.
    There are no built-ins — every node kind comes from an external package."""
    return {ep.name: ep.load() for ep in entry_points(group="dfio.nodes")}


def node_names() -> list[str]:
    return list(node_factories())


def build_node(name: str):
    factories = node_factories()
    if name not in factories:
        raise ValueError(
            f"Node type {name!r} not in registered dfio.nodes: {sorted(factories)}"
        )
    return factories[name]


def source_sink_schemes() -> list[str]:
    return [s for p in uri_parsers() for s in p.schemes]


def transform_schemes() -> list[str]:
    return [s for p in transformer_parsers() for s in p.schemes]


def build_source_sink(uri: ParsedUri, engine: Engine) -> object:
    for parser in uri_parsers():
        if parser.is_defined_at(uri):
            return parser.build(uri, engine)
    scheme = uri.scheme_and_name()[0]
    raise ValueError(
        f"URI scheme {scheme!r} not in supported schemes: {source_sink_schemes()}"
    )


def build_transformer(uri: ParsedUri):
    for parser in transformer_parsers():
        if parser.is_defined_at(uri):
            return parser.build(uri)
    scheme = uri.scheme_source_sink()[0]
    raise ValueError(
        f"Transformer scheme {scheme!r} not in supported schemes: {transform_schemes()}"
    )


def as_source(obj: object) -> DataFrameSource:
    assert isinstance(obj, DataFrameSource), f"{obj!r} is not a DataFrameSource"
    return obj


def as_sink(obj: object) -> DataFrameSink:
    assert isinstance(obj, DataFrameSink), f"{obj!r} is not a DataFrameSink"
    return obj
