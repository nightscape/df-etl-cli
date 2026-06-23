import ibis
import ibis.expr.datatypes as dt
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from conftest import records, tables
from dfio.engine import Engine
from dfio.sources.parquet import ParquetSourceSink
from dfio.sources.text import TextFileSourceSink, TextUriParser
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


def _write_csv(path) -> str:
    p = str(path / "data.csv")
    # blank cells in two columns; a numeric-looking and a text column
    open(p, "w").write("a,b,c\n1,,x\n2,hi,\n")
    return p


@pytest.mark.parametrize("backend", ["duckdb", "polars"])
def test_text_csv_string_faithful_infer_false_empty_string(backend, tmp_path):
    """Feature C: infer=false&empty=string → all-string columns, '' never null,
    identical across backends."""
    path = _write_csv(tmp_path)
    engine = Engine.from_config(backend)
    uri = ParsedUri.parse(f"text://{path}?infer=false&empty=string")
    table = TextUriParser().build(uri, engine).read()
    engine.register("t", table)
    out = engine.table("t").execute()
    assert [str(t) for t in table.schema().types] == ["string", "string", "string"]
    assert out["a"].tolist() == ["1", "2"]  # numeric-looking column stays string
    assert out["b"].tolist() == ["", "hi"]
    assert out["c"].tolist() == ["x", ""]
    assert None not in out["b"].tolist() and None not in out["c"].tolist()


def test_text_csv_default_infers_types(tmp_path):
    """Default read (no params) infers types — column `a` becomes int, which is
    exactly the type fidelity `infer=false` exists to suppress."""
    path = _write_csv(tmp_path)
    engine = Engine.from_config("duckdb")
    table = TextUriParser().build(ParsedUri.parse(f"text://{path}"), engine).read()
    assert str(table.schema()["a"]).startswith("int")  # inferred numeric, not string


def test_empty_string_requires_infer_false(tmp_path):
    path = _write_csv(tmp_path)
    engine = Engine.from_config("duckdb")
    with pytest.raises(AssertionError, match="empty=string requires infer=false"):
        TextUriParser().build(ParsedUri.parse(f"text://{path}?empty=string"), engine)


def test_text_csv_semicolon_delimiter_param(tmp_path):
    """0b: a .csv with ; separators reads as separate columns when delimiter=';'."""
    path = str(tmp_path / "vmt.csv")
    open(path, "w").write("a;b;c\n1;;x\n")
    engine = Engine.from_config("duckdb")
    uri = ParsedUri.parse(f"text://{path}?delimiter=;&infer=false&empty=string")
    table = TextUriParser().build(uri, engine).read()
    assert table.schema().names == ("a", "b", "c")
    out = table.execute()
    assert out["b"].tolist() == [""]  # blank stays empty string, not null


def test_read_csv_helper_string_faithful(tmp_path):
    """Feature C as a function: read_csv returns a polars frame, '' not null."""
    import dfio

    path = str(tmp_path / "x.csv")
    open(path, "w").write("a;b\n1;\n")
    df = dfio.read_csv(path, infer=False, empty="string", delimiter=";")
    assert df.columns == ["a", "b"]
    assert df["a"].dtype == __import__("polars").String
    assert df["b"].to_list() == [""]


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
