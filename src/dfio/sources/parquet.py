"""parquet:// — Parquet files. Ports ParquetDataFrameIO.scala.

Unlike the Scala version, write does not swallow exceptions — failures raise.
"""

from __future__ import annotations

import ibis

from ..base import UriParser
from ..engine import Engine
from ..uri import ParsedUri


class ParquetSourceSink:
    def __init__(self, engine: Engine, path: str):
        self.engine = engine
        self.path = path

    def read(self) -> ibis.Table:
        return self.engine.con.read_parquet(self.path)

    def write(self, table: ibis.Table) -> bool:
        self.engine.con.to_parquet(table, self.path)
        return True


class ParquetUriParser(UriParser):
    @property
    def schemes(self) -> list[str]:
        return ["parquet"]

    def build(self, uri: ParsedUri, engine: Engine) -> ParquetSourceSink:
        return ParquetSourceSink(engine, uri.path)
