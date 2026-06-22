from urllib.parse import quote

import ibis
import pytest

from dfio import ETL, Sink, Source, Transformation


def _read_parquet(path: str):
    return ibis.duckdb.connect().read_parquet(path).order_by("id").execute()


@pytest.mark.parametrize("engine", ["duckdb", "polars"])
def test_values_sql_parquet_pipeline(engine, tmp_path):
    out = str(tmp_path / "out.parquet")
    sql = quote("SELECT id, name FROM data WHERE age > 28")
    ETL(
        sources=[
            Source.parse(
                "data+values:///?header=id:long,name:string,age:int"
                "&values=1,Alice,30;2,Bob,25;3,Charlie,35"
            )
        ],
        transforms=[Transformation.parse(f"data+out+sql:///{sql}")],
        sinks=[Sink.parse(f"out+parquet://{out}")],
        backend=engine,
    ).run()
    df = _read_parquet(out)
    assert df["id"].tolist() == [1, 3]
    assert df["name"].tolist() == ["Alice", "Charlie"]


def test_default_identity_transform_copies_source_to_sink(tmp_path):
    out = str(tmp_path / "copy.parquet")
    ETL(
        sources=[Source.parse("source+values:///?header=id:long&values=1;2;3")],
        transforms=[],  # defaults to identity source -> sink
        sinks=[Sink.parse(f"sink+parquet://{out}")],
    ).run()
    assert _read_parquet(out)["id"].tolist() == [1, 2, 3]


def test_unknown_scheme_raises():
    with pytest.raises(ValueError, match="not in supported schemes"):
        Source.parse("bogus://x").run(__import__("dfio").Engine.from_config())
