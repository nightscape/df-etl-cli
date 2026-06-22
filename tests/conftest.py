"""Shared Hypothesis strategies for generating small tabular data."""

from __future__ import annotations

from hypothesis import strategies as st

# SQL/identifier-safe column names: start with a letter, ascii alnum + underscore.
column_names = st.from_regex(r"[a-z][a-z0-9_]{0,7}", fullmatch=True)

int_values = st.integers(min_value=-1000, max_value=1000)


def _strings(min_size: int):
    # lowercase ascii only, avoiding CSV/values metacharacters and quoting edge cases
    return st.text(
        alphabet=st.characters(min_codepoint=97, max_codepoint=122),
        min_size=min_size,
        max_size=8,
    )


@st.composite
def tables(draw, min_cols: int = 1, max_cols: int = 4, max_rows: int = 6, min_str_size: int = 0):
    """A dict-of-columns table with unique column names and a uniform row count.

    Each column is either int or string; all columns share the same length.
    ``min_str_size=1`` excludes empty strings — CSV cannot distinguish an empty
    string from null, so the text round-trip property is tested without them.
    """
    names = draw(
        st.lists(column_names, min_size=min_cols, max_size=max_cols, unique=True)
    )
    n_rows = draw(st.integers(min_value=0, max_value=max_rows))
    str_values = _strings(min_str_size)
    columns: dict[str, list] = {}
    for name in names:
        value_strategy = draw(st.sampled_from([int_values, str_values]))
        columns[name] = draw(
            st.lists(value_strategy, min_size=n_rows, max_size=n_rows)
        )
    return columns


def records(columns: dict[str, list]) -> list[tuple]:
    """Order-independent comparison key: sorted list of row tuples (as strings)."""
    names = list(columns)
    n = len(columns[names[0]]) if names else 0
    rows = [tuple(str(columns[name][i]) for name in names) for i in range(n)]
    return sorted(rows)
