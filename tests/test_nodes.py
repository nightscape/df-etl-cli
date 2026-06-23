"""Domain-node model (features F+G), run_graph, caching/--only, runtime."""

from __future__ import annotations

import os

import polars as pl
import pytest

from dfio import Engine, Graph, registry, run_graph


@pytest.fixture
def nodes(monkeypatch):
    """Register a set of in-process ``dfio.nodes`` plugins for one test."""

    def _register(mapping):
        monkeypatch.setattr(registry, "node_factories", lambda: mapping)

    return _register


def _double(ctx):
    ctx.emit(ctx.input.with_columns((pl.col("n") * 2).alias("n")))


def _incr(ctx):
    ctx.emit(ctx.input.with_columns((pl.col("n") + 1).alias("n")))


def _listify(ctx):
    ctx.emit(ctx.input.group_by("g").agg(pl.col("n").alias("ns")))


def _need_runtime(ctx):
    assert ctx.runtime == {"model": "x"}, ctx.runtime
    ctx.emit(ctx.input)


def _join_write(ctx):
    # multi-input pure sink: inputs preserve declaration order, never emits.
    a_id, b_id = list(ctx.inputs)
    assert (a_id, b_id) == ("sa", "sb")
    out = os.path.join(ctx.out_dir, ctx.params["out"])
    ctx.inputs[a_id].join(ctx.inputs[b_id], on="k").write_csv(out)


def _src(values="1;2;3"):
    return {"type": "values", "header": "n:long", "values": values}


def test_plugin_node_emits_and_downstream_reads(nodes):
    nodes({"double": _double, "incr": _incr})
    graph = Graph.from_dict({
        "data": {"src": _src()},
        "pipeline": [
            {"id": "a", "type": "double", "inputs": ["src"]},
            {"id": "b", "type": "incr", "inputs": ["a"]},
        ],
    })
    engine = Engine.from_config("duckdb")
    run_graph(graph, engine)
    out = engine.table("b").to_polars().sort("n")
    assert out["n"].to_list() == [3, 5, 7]  # (n*2)+1


def test_plugin_node_receives_runtime(nodes):
    nodes({"rt": _need_runtime})
    graph = Graph.from_dict({
        "data": {"src": _src()},
        "pipeline": [{"id": "a", "type": "rt", "inputs": ["src"]}],
    })
    run_graph(graph, Engine.from_config("duckdb"), runtime={"model": "x"})


def test_multi_input_pure_sink_ordered(nodes, tmp_path):
    nodes({"jw": _join_write})
    graph = Graph.from_dict({
        "data": {
            "sa": {"type": "values", "header": "k:long,a:string", "values": "1,x"},
            "sb": {"type": "values", "header": "k:long,b:string", "values": "1,y"},
        },
        "pipeline": [
            {"id": "j", "type": "jw", "inputs": ["sa", "sb"], "out": "pair.csv"},
        ],
    })
    run_graph(graph, Engine.from_config("duckdb"), out_dir=str(tmp_path))
    assert (tmp_path / "pair.csv").exists()


def test_cache_and_only_reuse_upstream(nodes, tmp_path):
    nodes({"double": _double, "incr": _incr})
    doc = {
        "data": {"src": _src()},
        "pipeline": [
            {"id": "a", "type": "double", "inputs": ["src"], "cache": True},
            {"id": "b", "type": "incr", "inputs": ["a"]},
        ],
    }
    out_dir = str(tmp_path)
    # Run 1: full graph caches `a`.
    run_graph(Graph.from_dict(doc), Engine.from_config("duckdb"), out_dir=out_dir)
    assert (tmp_path / "a.parquet").exists()

    # Run 2: fresh engine, only `b`; `a` loads from cache (never recomputed).
    engine2 = Engine.from_config("duckdb")
    run_graph(Graph.from_dict(doc), engine2, out_dir=out_dir, only=["b"])
    assert engine2.table("b").to_polars().sort("n")["n"].to_list() == [3, 5, 7]


def test_only_missing_cache_fails_fast(nodes, tmp_path):
    nodes({"double": _double, "incr": _incr})
    doc = {
        "data": {"src": _src()},
        "pipeline": [
            {"id": "a", "type": "double", "inputs": ["src"]},
            {"id": "b", "type": "incr", "inputs": ["a"]},
        ],
    }
    with pytest.raises(AssertionError, match="run 'a' first"):
        run_graph(Graph.from_dict(doc), Engine.from_config("duckdb"),
                  out_dir=str(tmp_path), only=["b"])


def test_list_column_survives_cache_roundtrip(nodes, tmp_path):
    nodes({"listify": _listify})
    graph = Graph.from_dict({
        "data": {"src": {"type": "values", "header": "g:long,n:long",
                         "values": "1,10;1,11;2,20"}},
        "pipeline": [{"id": "a", "type": "listify", "inputs": ["src"], "cache": True}],
    })
    run_graph(graph, Engine.from_config("duckdb"), out_dir=str(tmp_path))
    reloaded = pl.read_parquet(tmp_path / "a.parquet")
    assert reloaded.schema["ns"] == pl.List(pl.Int64)


def test_input_sugar_asserts_single(nodes):
    seen = {}

    def _grab(ctx):
        seen["n"] = len(ctx.inputs)
        ctx.emit(ctx.input)

    nodes({"grab": _grab})
    graph = Graph.from_dict({
        "data": {"src": _src()},
        "pipeline": [{"id": "a", "type": "grab", "inputs": ["src"]}],
    })
    run_graph(graph, Engine.from_config("duckdb"))
    assert seen["n"] == 1
