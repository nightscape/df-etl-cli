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
import pyarrow as pa
import pyarrow.csv as pacsv

from ..base import UriParser
from ..engine import Engine
from ..uri import ParsedUri


def _delimiter_for(path: str) -> str:
    return "\t" if path.rsplit(".", 1)[-1].lower() == "tsv" else ","


def read_csv(path, *, infer=True, empty=None, delimiter=None, header=True):
    """String-faithful CSV read outside a graph (feature C as a function).

    The same pyarrow read the ``text`` source does, returned as a
    ``polars.DataFrame``. ``empty="string"`` materializes blank cells as ``""``
    (requires ``infer=False``); ``delimiter`` overrides the extension default.
    One CSV codepath: a thin wrapper over ``TextFileSourceSink.read``.
    """
    empty_as_string = empty == "string"
    sink = TextFileSourceSink(
        engine=None,
        path=path,
        delimiter=delimiter or _delimiter_for(path),
        header=header,
        infer=infer,
        empty_as_string=empty_as_string,
    )
    return sink.read().to_polars()


class TextFileSourceSink:
    def __init__(
        self,
        engine: Engine,
        path: str,
        delimiter: str,
        header: bool,
        infer: bool = True,
        empty_as_string: bool = False,
    ):
        self.engine = engine
        self.path = path
        self.delimiter = delimiter
        self.header = header
        # String-faithful CSV (feature C): with ``infer=False`` every column is
        # read as string; with ``empty_as_string`` blank cells stay ``""`` rather
        # than null. The two only make sense together — blanks in an *inferred*
        # numeric column have no string to fall back to.
        self.infer = infer
        self.empty_as_string = empty_as_string

    def read(self) -> ibis.Table:
        read_options = pacsv.ReadOptions(autogenerate_column_names=not self.header)
        parse_options = pacsv.ParseOptions(delimiter=self.delimiter)
        convert_options = self._convert_options(read_options, parse_options)
        arrow = pacsv.read_csv(
            self.path,
            read_options=read_options,
            parse_options=parse_options,
            convert_options=convert_options,
        )
        return ibis.memtable(arrow)

    def _convert_options(self, read_options, parse_options):
        if self.infer and not self.empty_as_string:
            return None
        kwargs: dict = {}
        if not self.infer:
            # Pre-read the names with the SAME options the real read uses, so a
            # .tsv or headerless file gets the right column set, then pin every
            # column to string.
            names = pacsv.open_csv(
                self.path, read_options=read_options, parse_options=parse_options
            ).schema.names
            kwargs["column_types"] = {name: pa.string() for name in names}
        if self.empty_as_string:
            kwargs["strings_can_be_null"] = False
            kwargs["null_values"] = []
        return pacsv.ConvertOptions(**kwargs)

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
        qp = uri.query_params
        header = qp.get("header", "true").lower() == "true"
        # Delimiter is extension-derived by default; ``delimiter``/``sep`` overrides
        # it (the VMT export is a ``.csv`` with ``;`` separators).
        delimiter = qp.get("delimiter") or qp.get("sep") or _delimiter_for(uri.path)
        infer = not (
            qp.get("infer", "true").lower() == "false"
            or qp.get("schema", "").lower() == "string"
            or qp.get("all_varchar", "").lower() == "true"
        )
        empty_as_string = (
            qp.get("empty", "").lower() == "string"
            or qp.get("null_as_empty", "").lower() == "true"
        )
        assert not (empty_as_string and infer), (
            f"text://{uri.path}: empty=string requires infer=false "
            "(blank cells in an inferred non-string column have no string fallback)"
        )
        return TextFileSourceSink(
            engine, uri.path, delimiter, header, infer, empty_as_string
        )
