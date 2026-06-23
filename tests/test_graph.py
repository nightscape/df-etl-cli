"""Feature A (graph + DAG runner) and B (inline params)."""

import json

import ibis
import pytest
import yaml

from dfio.engine import Engine
from dfio.graph import Graph
from dfio.runner import run_nodes

FANOUT = {
    "data": {
        "src": {"type": "values", "header": "id:long,name:string", "values": "1,a;2,b"},
    },
    "pipeline": [
        {"id": "up", "type": "sql", "inputs": ["src"],
         "query": "SELECT id, upper(name) AS name FROM src"},
        {"id": "lo", "type": "sql", "inputs": ["src"],
         "query": "SELECT id, lower(name) AS name FROM src"},
    ],
}


def _run(doc) -> Engine:
    engine = Engine.from_config("duckdb")
    run_nodes(Graph.from_dict(doc).compile(), engine)
    return engine


def test_inline_params_are_stringified():
    """0a: native YAML scalars become the strings scheme parsers expect, so a
    bool `infer: false` no longer crashes `.lower()`."""
    from dfio.graph import NodeSpec

    spec = NodeSpec.from_dict(
        {"type": "text", "path": "x.csv", "infer": False, "header": True, "topn": 50},
        id="v", is_source=True,
    )
    assert spec.params == {"path": "x.csv", "infer": "false", "header": "true", "topn": "50"}


def test_graph_engine_key_and_bind():
    """Feature: top-level engine key (default duckdb) and Graph.bind."""
    g = Graph.from_dict({
        "engine": "polars",
        "data": {"src": {"type": "text", "path": "old.csv"}},
        "pipeline": [{"id": "out", "type": "parquet", "inputs": ["src"],
                      "path": "o.parquet", "sink": True}],
    })
    assert g.engine == "polars"
    g.bind("src", "new.csv")
    assert g.sources["src"].params["path"] == "new.csv"
    with pytest.raises(AssertionError, match="unknown source"):
        g.bind("nope", "x.csv")


def test_default_engine_is_duckdb():
    assert Graph.from_dict({"data": {}, "pipeline": []}).engine == "duckdb"


def test_fanout_one_source_feeds_two_transforms():
    engine = _run(FANOUT)
    assert sorted(engine.table("up")["name"].execute().tolist()) == ["A", "B"]
    assert sorted(engine.table("lo")["name"].execute().tolist()) == ["a", "b"]


def test_json_and_yaml_load_identically(tmp_path):
    jpath = tmp_path / "g.json"
    ypath = tmp_path / "g.yaml"
    jpath.write_text(json.dumps(FANOUT))
    ypath.write_text(yaml.safe_dump(FANOUT))
    nj = [n.id for n in Graph.load(str(jpath)).compile()]
    ny = [n.id for n in Graph.load(str(ypath)).compile()]
    assert nj == ny == ["src", "up", "lo"]


def test_cyclic_graph_rejected_with_ids():
    doc = {
        "pipeline": [
            {"id": "a", "type": "sql", "inputs": ["b"], "query": "SELECT * FROM b"},
            {"id": "b", "type": "sql", "inputs": ["a"], "query": "SELECT * FROM a"},
        ]
    }
    with pytest.raises(ValueError, match=r"Cycle.*'a'.*'b'|Cycle.*\['a', 'b'\]"):
        Graph.from_dict(doc).validate()


def test_dangling_input_rejected_with_offending_id():
    doc = {
        "data": {"src": {"type": "values", "header": "id:long", "values": "1"}},
        "pipeline": [
            {"id": "t", "type": "sql", "inputs": ["nope"], "query": "SELECT * FROM nope"},
        ],
    }
    with pytest.raises(AssertionError, match="'nope'"):
        Graph.from_dict(doc).validate()


def test_unknown_extension_rejected(tmp_path):
    p = tmp_path / "g.txt"
    p.write_text("{}")
    with pytest.raises(ValueError, match="extension"):
        Graph.load(str(p))


def test_node_with_both_uri_and_params_rejected():
    doc = {"pipeline": [
        {"id": "t", "type": "sql", "inputs": ["x"], "uri": "sql:///SELECT 1", "query": "x"},
    ]}
    with pytest.raises(AssertionError, match="exactly one"):
        Graph.from_dict(doc)


def test_inline_sql_with_spaces_parens_and_regex_backslashes():
    """Feature B: a multi-line query with metacharacters runs without encoding."""
    doc = {
        "data": {
            "src": {"type": "values", "header": "id:long,label:string",
                    "values": "1,foo(bar);2,baz"},
        },
        "pipeline": [
            {"id": "derived", "type": "sql", "inputs": ["src"], "query": (
                "SELECT id,\n"
                "       regexp_extract(label, '\\(([^)]+)\\)', 1) AS inside\n"
                "FROM src\n"
                "WHERE label LIKE '%(%'"
            )},
        ],
    }
    engine = _run(doc)
    rows = engine.table("derived").execute()
    assert rows["inside"].tolist() == ["bar"]


def test_end_to_end_csv_to_parquet_sink(tmp_path):
    csv = tmp_path / "in.csv"
    csv.write_text("id,age\n1,30\n2,25\n3,40\n")
    out = tmp_path / "out.parquet"
    doc = {
        "data": {"people": {"type": "text", "path": str(csv)}},
        "pipeline": [
            {"id": "adults", "type": "sql", "inputs": ["people"],
             "query": "SELECT id FROM people WHERE age > 28"},
            {"id": "write", "type": "parquet", "inputs": ["adults"],
             "path": str(out), "sink": True},
        ],
    }
    _run(doc)
    back = ibis.duckdb.connect().read_parquet(str(out)).order_by("id").execute()
    assert back["id"].tolist() == [1, 3]
