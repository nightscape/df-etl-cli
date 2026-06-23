from urllib.parse import quote

import ibis

from dfio.cli import main


def test_cli_csv_to_parquet(tmp_path):
    src = tmp_path / "in.csv"
    src.write_text("n,name\n1,a\n2,b\n3,c\n")
    out = str(tmp_path / "out.parquet")
    sql = quote("SELECT * FROM data WHERE n > 1")
    main(
        [
            "--source", f"data+text://{src}",
            "--transform", f"data+out+sql:///{sql}",
            "--sink", f"out+parquet://{out}",
        ]
    )
    df = ibis.duckdb.connect().read_parquet(out).order_by("n").execute()
    assert df["n"].tolist() == [2, 3]
    assert df["name"].tolist() == ["b", "c"]


def test_cli_graph_bind_and_out_dir(tmp_path):
    """Feature I/J: --bind rebinds a source path, relative sink resolves under
    --out-dir, without editing the graph file."""
    real = tmp_path / "fresh.csv"
    real.write_text("n,name\n1,a\n2,b\n")
    graph = tmp_path / "g.yaml"
    graph.write_text(
        "data:\n"
        "  data: {type: text, path: placeholder.csv}\n"
        "pipeline:\n"
        "  - {id: out, type: parquet, inputs: [data], path: out.parquet, sink: true}\n"
    )
    out_dir = tmp_path / "run"
    main([
        "--graph", str(graph),
        "--bind", f"data={real}",
        "--out-dir", str(out_dir),
    ])
    df = ibis.duckdb.connect().read_parquet(str(out_dir / "out.parquet")).order_by("n").execute()
    assert df["name"].tolist() == ["a", "b"]


def test_cli_bind_requires_graph(tmp_path):
    import pytest

    with pytest.raises(AssertionError, match="require --graph"):
        main(["--source", "data+text://x.csv", "--bind", "data=y.csv"])
