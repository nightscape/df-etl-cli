"""flatten:// and flatten-explode:// — unnest nested data. Ports the two
flatten transformers in TransformerParser.scala.

- ``flatten`` recursively unpacks struct columns into ``parent_child`` columns.
- ``flatten-explode`` additionally explodes array columns into rows.

Nested-type handling is backend-dependent; developed/verified against duckdb.
"""

from __future__ import annotations

import ibis
import ibis.expr.datatypes as dt

from ..base import Transformer, TransformerParser
from ..uri import ParsedUri


def _struct_exprs(col, name: str, dtype: dt.DataType):
    """Flat list of (alias, expr) for a possibly-nested struct column."""
    if isinstance(dtype, dt.Struct):
        out = []
        for field_name, field_type in dtype.items():
            out += _struct_exprs(col[field_name], f"{name}_{field_name}", field_type)
        return out
    return [(name, col)]


def flatten(table: ibis.Table) -> ibis.Table:
    exprs: dict[str, object] = {}
    for name in table.columns:
        col = table[name]
        for alias, expr in _struct_exprs(col, name, col.type()):
            exprs[alias] = expr
    return table.select(**exprs) if exprs else table


def flatten_explode(table: ibis.Table) -> ibis.Table:
    """Iteratively flatten one struct or explode one array until neither remains."""
    while True:
        struct_col = next(
            (c for c in table.columns if isinstance(table[c].type(), dt.Struct)), None
        )
        if struct_col is not None:
            dtype = table[struct_col].type()
            projection = {c: table[c] for c in table.columns if c != struct_col}
            for field_name, _ in dtype.items():
                projection[f"{struct_col}_{field_name}"] = table[struct_col][field_name]
            table = table.select(**projection)
            continue

        array_col = next(
            (c for c in table.columns if isinstance(table[c].type(), dt.Array)), None
        )
        if array_col is not None:
            table = table.unnest(array_col)
            continue

        return table


class FlattenParser(TransformerParser):
    @property
    def schemes(self) -> list[str]:
        return ["flatten"]

    def build(self, uri: ParsedUri) -> Transformer:
        return flatten


class FlattenAndExplodeParser(TransformerParser):
    @property
    def schemes(self) -> list[str]:
        return ["flatten-explode"]

    def build(self, uri: ParsedUri) -> Transformer:
        return flatten_explode
