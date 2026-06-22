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
