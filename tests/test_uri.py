from hypothesis import given
from hypothesis import strategies as st

from dfio.uri import ParsedUri

# Names live in the URI scheme position, so they must be valid scheme chars
# (hyphens allowed, underscores are not — those only appear after normalization).
names = st.from_regex(r"[a-z][a-z0-9-]{0,7}", fullmatch=True)
schemes = st.sampled_from(["text", "parquet", "values", "console", "sql"])


@given(names, schemes)
def test_scheme_and_name_roundtrip(name, scheme):
    uri = ParsedUri.parse(f"{name}+{scheme}://host/path?a=1")
    assert uri.scheme_and_name() == (scheme, name)


@given(schemes)
def test_scheme_without_name(scheme):
    uri = ParsedUri.parse(f"{scheme}:///path")
    assert uri.scheme_and_name() == (scheme, None)


@given(names, names, schemes)
def test_scheme_source_sink_roundtrip(source, sink, scheme):
    uri = ParsedUri.parse(f"{source}+{sink}+{scheme}:///q")
    assert uri.scheme_source_sink() == (scheme, source, sink)


@given(
    st.lists(
        st.tuples(st.from_regex(r"[a-z]{1,5}", fullmatch=True), st.integers(0, 99)),
        min_size=1,
        max_size=4,
        unique_by=lambda kv: kv[0],
    )
)
def test_query_params(pairs):
    query = "&".join(f"{k}={v}" for k, v in pairs)
    uri = ParsedUri.parse(f"text:///f.csv?{query}")
    assert uri.query_params == {k: str(v) for k, v in pairs}
