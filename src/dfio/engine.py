"""The execution engine: an Ibis backend connection plus a named-table catalog.

Replaces Spark's ``SparkSession``. ``register``/``table`` mirror
``createOrReplaceTempView``/``spark.table`` — named tables are also registered as
views in the backend so the SQL transformer can reference them by name.
"""

from __future__ import annotations

import ibis
from ibis.backends import BaseBackend


class Engine:
    def __init__(self, backend: BaseBackend, name: str = "duckdb"):
        self.con = backend
        self.backend_name = name
        self._catalog: dict[str, ibis.Table] = {}

    @classmethod
    def from_config(cls, backend: str = "duckdb") -> "Engine":
        con = ibis.connect(f"{backend}://")
        return cls(con, name=backend)

    def register(self, name: str, table: ibis.Table) -> ibis.Table:
        """Register ``table`` under ``name`` and expose it as a backend view."""
        view = self.con.create_view(name, table, overwrite=True)
        self._catalog[name] = view
        return view

    def has(self, name: str) -> bool:
        return name in self._catalog

    def table(self, name: str) -> ibis.Table:
        if name not in self._catalog:
            raise KeyError(
                f"No table named {name!r} in catalog; known: {sorted(self._catalog)}"
            )
        return self._catalog[name]
