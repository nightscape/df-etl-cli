import ibis
import ibis.expr.datatypes as dt
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from conftest import records, tables
from dfio.engine import Engine
from dfio.sources.parquet import ParquetSourceSink
from dfio.sources.text import TextFileSourceSink
from dfio.sources.values import ValuesUriParser
from dfio.uri import ParsedUri

NO_FUNCTION_SCOPED = settings(
    suppress_health_check=[HealthCheck.function_scoped_fixture], deadline=None
)


@pytest.fixture
def engine():
    return Engine.from_config("duckdb")


@NO_FUNCTION_SCOPED
@given(tables())
def test_parquet_roundtrip(engine, tmp_path_factory, columns):
    path = str(tmp_path_factory.mktemp("pq") / "data.parquet")
    table = ibis.memtable(columns)
    sink = ParquetSourceSink(engine, path)
    assert sink.write(table)
    back = ParquetSourceSink(engine, path).read()
    assert records(back.execute().to_dict("list")) == records(columns)


@NO_FUNCTION_SCOPED
@given(tables(min_str_size=1))
def test_text_csv_roundtrip(engine, tmp_path_factory, columns):
    path = str(tmp_path_factory.mktemp("csv") / "data.csv")
    table = ibis.memtable(columns)
    sink = TextFileSourceSink(engine, path, delimiter=",", header=True)
    assert sink.write(table)
    back = TextFileSourceSink(engine, path, delimiter=",", header=True).read()
    assert records(back.execute().to_dict("list")) == records(columns)


def test_text_csv_empty_string_reads_back_as_null(engine, tmp_path):
    """Known CSV limitation: an empty string is written as an empty field and is
    indistinguishable from null, so it reads back as null (unlike parquet)."""
    path = str(tmp_path / "data.csv")
    TextFileSourceSink(engine, path, delimiter=",", header=True).write(
        ibis.memtable({"a": [""]})
    )
    back = TextFileSourceSink(engine, path, delimiter=",", header=True).read()
    assert back.execute()["a"].tolist() == [None]


_TYPE_MAP = {"int": dt.int32, "long": dt.int64, "double": dt.float64, "string": dt.string}


@given(
    st.dictionaries(
        st.from_regex(r"[a-z][a-z0-9_]{0,7}", fullmatch=True),
        st.sampled_from(list(_TYPE_MAP)),
        min_size=1,
        max_size=4,
    )
)
def test_values_schema_typing(spec):
    engine = Engine.from_config("duckdb")
    header = ",".join(f"{name}:{type_name}" for name, type_name in spec.items())
    uri = ParsedUri.parse(f"values:///?header={header}")
    table = ValuesUriParser().build(uri, engine).read()
    expected = ibis.schema({name: _TYPE_MAP[type_name] for name, type_name in spec.items()})
    assert table.schema() == expected
