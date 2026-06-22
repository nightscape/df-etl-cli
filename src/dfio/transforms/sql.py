"""sql:// and sql-file:// — run SQL against the engine catalog.

Ports SqlTransformerParser. ``sql://`` carries url-encoded SQL in the path;
``sql-file://`` reads the query from a file. The query references previously
registered named tables (see Engine.register), so it runs via the backend
connection rather than against the single input table.

Because the transformer signature is ``Table -> Table``, we recover the backend
connection from the input table (``ibis.get_backend``) and execute
``con.sql(query)`` so the query can reference every registered named view, not
just the input table.

The input SQL is parsed in a fixed dialect (``duckdb`` by default, overridable
with ``?dialect=``) rather than each backend's own dialect. Ibis transpiles that
single parse to whichever engine actually runs, so one SQL string means the same
thing on every backend instead of being reinterpreted per engine.
"""

from __future__ import annotations

from pathlib import Path

import ibis

from ..base import Transformer, TransformerParser
from ..uri import ParsedUri


class SqlParser(TransformerParser):
    @property
    def schemes(self) -> list[str]:
        return ["sql", "sql-file"]

    def build(self, uri: ParsedUri) -> Transformer:
        scheme = uri.scheme_source_sink()[0]
        if scheme == "sql":
            # path already url-decoded by ParsedUri; drop the leading "/"
            query = uri.path.lstrip("/")
        else:
            query = Path(uri.path).read_text()
        assert query.strip(), (
            f"Empty SQL for {uri.raw!r}; inline SQL must follow 'sql:///' and be "
            "url-encoded (e.g. spaces as %20)"
        )
        dialect = uri.query_params.get("dialect", "duckdb")

        def run(table: ibis.Table) -> ibis.Table:
            return ibis.get_backend(table).sql(query, dialect=dialect)

        return run
