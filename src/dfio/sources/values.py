"""values:// — in-memory table from URI params. Ports ValuesSource.scala.

    values:///?header=foo:int,bar:string&values=1,a;2,b

``header`` declares typed columns; ``values`` is ``;``-separated rows of
``,``-separated cells. Typing is done by building an ibis memtable of strings and
casting to the declared schema (replaces the Scala ``convertValue``).
"""

from __future__ import annotations

import ibis

from ..base import UriParser
from ..engine import Engine
from ..types import parse_schema_spec
from ..uri import ParsedUri


class ValuesSourceSink:
    def __init__(self, engine: Engine, schema: dict, rows: list[list[str]]):
        self.engine = engine
        self.schema = schema
        self.rows = rows

    def read(self) -> ibis.Table:
        names = list(self.schema)
        columns = {name: [row[i] for row in self.rows] for i, name in enumerate(names)}
        table = ibis.memtable(columns, schema={name: "string" for name in names})
        return table.cast(self.schema)

    def write(self, table: ibis.Table) -> bool:
        print(table.head(10_000).execute().to_string())
        return True


class ValuesUriParser(UriParser):
    @property
    def schemes(self) -> list[str]:
        return ["values"]

    def build(self, uri: ParsedUri, engine: Engine) -> ValuesSourceSink:
        params = uri.query_params
        schema = parse_schema_spec(params.get("header", ""))
        raw = params.get("values", "")
        rows = [cell_row.split(",") for cell_row in raw.split(";") if cell_row]
        return ValuesSourceSink(engine, schema, rows)
