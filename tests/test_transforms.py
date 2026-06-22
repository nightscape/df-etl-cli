import ibis
import pytest
from hypothesis import HealthCheck, given, settings

from conftest import records, tables
from dfio.engine import Engine
from dfio.transforms.flatten import flatten, flatten_explode
from dfio.transforms.sql import SqlParser
from dfio.uri import ParsedUri

NO_FUNCTION_SCOPED = settings(
    suppress_health_check=[HealthCheck.function_scoped_fixture], deadline=None
)


@NO_FUNCTION_SCOPED
@given(tables())
def test_sql_select_star_is_identity(columns):
    con = ibis.duckdb.connect()
    con.create_view("source", ibis.memtable(columns), overwrite=True)
    out = con.sql("SELECT * FROM source")
    assert records(out.execute().to_dict("list")) == records(columns)


@pytest.mark.parametrize("engine", ["duckdb", "polars"])
def test_sql_transform_consistent_across_engines(engine):
    """The same SQL string, run through SqlParser, yields the same result on
    every backend — the input dialect is pinned so it is not reinterpreted."""
    eng = Engine.from_config(engine)
    eng.register("source", ibis.memtable({"a": ["x", "y"], "b": ["1", "2"]}))
    transform = SqlParser().build(ParsedUri.parse("sql:///SELECT a || b AS c FROM source"))
    out = transform(eng.table("source"))
    assert sorted(out.execute()["c"].tolist()) == ["x1", "y2"]


@NO_FUNCTION_SCOPED
@given(tables())
def test_flatten_is_identity_without_nesting(columns):
    # No struct/array columns are generated, so flatten must be a no-op.
    table = ibis.memtable(columns)
    out = flatten(table)
    assert sorted(out.columns) == sorted(columns)
    assert records(out.execute().to_dict("list")) == records(columns)


def test_flatten_unpacks_nested_struct():
    table = ibis.memtable({"id": [1], "info": [{"x": 10, "y": {"z": 20}}]})
    out = flatten(table)
    assert set(out.columns) == {"id", "info_x", "info_y_z"}
    row = out.execute().to_dict("records")[0]
    assert (row["info_x"], row["info_y_z"]) == (10, 20)


def test_flatten_explode_explodes_arrays_and_preserves_other_rows():
    table = ibis.memtable({"id": [1], "tags": [["a", "b", "c"]]})
    out = flatten_explode(table)
    df = out.execute()
    assert len(df) == 3
    assert sorted(df["tags"].tolist()) == ["a", "b", "c"]
    assert df["id"].tolist() == [1, 1, 1]
