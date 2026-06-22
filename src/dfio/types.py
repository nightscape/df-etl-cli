"""Parse the ``name:type`` mini-spec into an ibis schema.

Ports the type handling done by hand in ``ValuesSource.scala`` (``convertValue``
and the ``StructField`` parsing). Supported types mirror the Scala set; anything
unrecognized falls back to ``string``.
"""

from __future__ import annotations

import ibis.expr.datatypes as dt

_TYPES: dict[str, dt.DataType] = {
    "int": dt.int32,
    "long": dt.int64,
    "double": dt.float64,
    "string": dt.string,
}


def parse_type(name: str) -> dt.DataType:
    return _TYPES.get(name.strip().lower(), dt.string)


def parse_schema_spec(spec: str) -> dict[str, dt.DataType]:
    """``"a:int,b:string"`` -> ``{"a": int32, "b": string}``.

    A bare ``name`` (no ``:type``) defaults to string, matching the Scala parser.
    """
    fields: dict[str, dt.DataType] = {}
    for field in spec.split(","):
        field = field.strip()
        if not field:
            continue
        name, sep, type_name = field.partition(":")
        fields[name.strip()] = parse_type(type_name) if sep else dt.string
    return fields
