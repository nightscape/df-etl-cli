"""Core abstractions, ported from the Scala traits.

- ``DataFrameSource`` / ``DataFrameSink``  (DataFrameSource.scala, DataFrameSink.scala)
- ``UriParser``                            (DataFrameUriParser.scala)
- ``TransformerParser``                    (TransformerParser.scala)

A ``UriParser`` builds a source/sink from a parsed URI and the engine. A
``TransformerParser`` builds a ``Table -> Table`` function.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Callable, Protocol, runtime_checkable

import ibis

from .engine import Engine
from .uri import ParsedUri

Transformer = Callable[[ibis.Table], ibis.Table]


@runtime_checkable
class DataFrameSource(Protocol):
    def read(self) -> ibis.Table: ...


@runtime_checkable
class DataFrameSink(Protocol):
    def write(self, table: ibis.Table) -> bool: ...


class UriParser(ABC):
    """Builds a source and/or sink for a set of URI schemes."""

    @property
    @abstractmethod
    def schemes(self) -> list[str]: ...

    def is_defined_at(self, uri: ParsedUri) -> bool:
        return uri.scheme_and_name()[0] in self.schemes

    @abstractmethod
    def build(self, uri: ParsedUri, engine: Engine) -> object:
        """Return an object implementing DataFrameSource and/or DataFrameSink."""


class TransformerParser(ABC):
    @property
    @abstractmethod
    def schemes(self) -> list[str]: ...

    def is_defined_at(self, uri: ParsedUri) -> bool:
        return uri.scheme_source_sink()[0] in self.schemes

    @abstractmethod
    def build(self, uri: ParsedUri) -> Transformer: ...
