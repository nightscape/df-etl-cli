"""The execution kernel: a ``Node`` protocol plus thin adapters.

Both front-ends — the linear ``--source/--transform/--sink`` CLI (``etl.py``) and
the declarative graph (``graph.py``) — compile down to a list of ``Node``s that
the runner (``runner.py``) executes in topological order. Each node reads its
inputs from the engine catalog by id and registers its output under its own id,
so the engine's named-table catalog is the single data plane.

The adapters reuse the existing build functions in ``registry.py`` verbatim; the
per-step ``run`` logic that used to live on ``etl.Source/Transformation/Sink``
lives here now, and those classes delegate to these adapters.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Protocol, runtime_checkable

import ibis

from .engine import Engine
from .registry import as_sink, as_source, build_source_sink, build_transformer
from .uri import ParsedUri

# Transformer schemes that legitimately take more than one input (the join is
# expressed in the query body, which resolves every catalog view by name). Every
# other Table -> Table transform takes exactly one primary input.
_MULTI_INPUT_SCHEMES = frozenset({"sql", "sql-file"})


@runtime_checkable
class Node(Protocol):
    id: str
    inputs: list[str]  # upstream node ids this node depends on

    def run(self, engine: Engine) -> None:
        """Compute and ``engine.register`` the output under ``self.id``."""
        ...


@dataclass
class SourceNode:
    id: str
    uri: ParsedUri
    inputs: list[str] = field(default_factory=list)

    def run(self, engine: Engine) -> None:
        source = as_source(build_source_sink(self.uri, engine))
        engine.register(self.id, source.read())


@dataclass
class TransformNode:
    id: str
    inputs: list[str]
    uri: ParsedUri

    def run(self, engine: Engine) -> None:
        scheme = self.uri.scheme_source_sink()[0]
        assert self.inputs, f"Transform node {self.id!r} has no input"
        assert scheme in _MULTI_INPUT_SCHEMES or len(self.inputs) == 1, (
            f"Transform node {self.id!r} (scheme {scheme!r}) takes one input but "
            f"got {self.inputs}; only {sorted(_MULTI_INPUT_SCHEMES)} accept several"
        )
        transformer = build_transformer(self.uri)
        output = transformer(engine.table(self.inputs[0]))
        engine.register(self.id, output)


@dataclass
class SinkNode:
    id: str
    inputs: list[str]
    uri: ParsedUri

    def run(self, engine: Engine) -> None:
        assert len(self.inputs) == 1, (
            f"Sink node {self.id!r} writes one input but got {self.inputs}"
        )
        sink = as_sink(build_source_sink(self.uri, engine))
        assert sink.write(engine.table(self.inputs[0])), (
            f"Writing to sink {self.id!r} failed"
        )


def _to_polars(table: ibis.Table):
    """Materialize an Ibis catalog table to a concrete ``polars.DataFrame`` — the
    boundary a plugin node reads its inputs across (features F/G)."""
    return table.to_polars()


@dataclass
class NodeContext:
    """What the runner hands each ``dfio.nodes`` plugin (features F+G).

    Polars-native: ``inputs`` values are ``polars.DataFrame`` and ``emit`` takes
    one. ``inputs`` preserves ``inputs:`` declaration order so a node can unpack
    ``raw_id, cluster_id = list(ctx.inputs)``. A plugin node may be a pure sink
    (writes a file, never calls ``emit``).
    """

    id: str
    inputs: dict  # ORDERED {upstream id: polars.DataFrame}, in `inputs:` order
    params: dict[str, str]
    out_dir: str | None
    runtime: object
    _emit: Callable[[str, object], None] = field(default=None, repr=False)

    @property
    def input(self):
        """Sugar for the single-input case."""
        assert len(self.inputs) == 1, (
            f"Node {self.id!r}.input needs exactly one input, got {list(self.inputs)}"
        )
        return next(iter(self.inputs.values()))

    def emit(self, df) -> None:
        """Register ``df`` (a ``polars.DataFrame``) as this node's output under
        ``self.id`` so downstream nodes can read it; ``cache: true`` nodes are also
        persisted to ``out_dir/<id>.parquet`` by the runner after this returns."""
        self._emit(self.id, df)


@dataclass
class PluginNode:
    """Adapter lowering a ``dfio.nodes`` pipeline node to the ``Node`` protocol.

    ``runtime``/``out_dir`` are injected by ``run_graph`` before ``run``; the
    linear ETL front-end never produces plugin nodes. Inputs are fully
    materialized to Polars *before* ``fn`` is invoked, so a bad upstream
    projection fails fast (before, e.g., ``link`` downloads multi-GB models)."""

    id: str
    inputs: list[str]
    fn: Callable[[NodeContext], None]
    params: dict[str, str] = field(default_factory=dict)
    cache: bool = False
    runtime: object = None
    out_dir: str | None = None

    def run(self, engine: Engine) -> None:
        frames = {i: _to_polars(engine.table(i)) for i in self.inputs}

        def _emit(node_id: str, df) -> None:
            engine.register(node_id, ibis.memtable(df))

        ctx = NodeContext(
            id=self.id,
            inputs=frames,
            params=self.params,
            out_dir=self.out_dir,
            runtime=self.runtime,
            _emit=_emit,
        )
        self.fn(ctx)
