"""text:// — CSV/TSV files. Ports TextFileDataFrameSource.scala.

Delimiter is derived from the file extension (``.csv`` -> ``,``, ``.tsv`` ->
``\\t``); ``?header=`` defaults to true.

IO goes through ``pyarrow.csv`` rather than each backend's ``read_csv``/``to_csv``
because those take backend-specific kwargs (duckdb ``delim`` vs polars
``separator``). Routing through pyarrow + ``ibis.memtable`` keeps ``text://``
identical across every Ibis backend, which is the whole point of the port.
"""

from __future__ import annotations

import ibis
import pyarrow.csv as pacsv

from ..base import UriParser
from ..engine import Engine
from ..uri import ParsedUri


def _delimiter_for(path: str) -> str:
    return "\t" if path.rsplit(".", 1)[-1].lower() == "tsv" else ","


class TextFileSourceSink:
    def __init__(self, engine: Engine, path: str, delimiter: str, header: bool):
        self.engine = engine
        self.path = path
        self.delimiter = delimiter
        self.header = header

    def read(self) -> ibis.Table:
        read_options = pacsv.ReadOptions(autogenerate_column_names=not self.header)
        parse_options = pacsv.ParseOptions(delimiter=self.delimiter)
        arrow = pacsv.read_csv(self.path, read_options=read_options, parse_options=parse_options)
        return ibis.memtable(arrow)

    def write(self, table: ibis.Table) -> bool:
        arrow = table.to_pyarrow()
        write_options = pacsv.WriteOptions(
            include_header=self.header, delimiter=self.delimiter
        )
        pacsv.write_csv(arrow, self.path, write_options=write_options)
        return True


class TextUriParser(UriParser):
    @property
    def schemes(self) -> list[str]:
        return ["text"]

    def build(self, uri: ParsedUri, engine: Engine) -> TextFileSourceSink:
        header = uri.query_params.get("header", "true").lower() == "true"
        return TextFileSourceSink(engine, uri.path, _delimiter_for(uri.path), header)
