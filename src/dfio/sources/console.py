"""console:// — empty source, preview-printing sink. Ports ConsoleDataFrameSink.scala."""

from __future__ import annotations

import ibis

from ..base import UriParser
from ..engine import Engine
from ..uri import ParsedUri

_PREVIEW_ROWS = 10_000


class ConsoleSourceSink:
    def __init__(self, engine: Engine):
        self.engine = engine

    def read(self) -> ibis.Table:
        return ibis.memtable({})

    def write(self, table: ibis.Table) -> bool:
        print(table.head(_PREVIEW_ROWS).execute().to_string())
        return True


class ConsoleUriParser(UriParser):
    @property
    def schemes(self) -> list[str]:
        return ["console"]

    def build(self, uri: ParsedUri, engine: Engine) -> ConsoleSourceSink:
        return ConsoleSourceSink(engine)
