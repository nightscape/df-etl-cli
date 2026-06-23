"""``dfio`` command line. Ports the ETL.scala mainargs interface.

    dfio --source name+scheme://...  --transform src+sink+scheme://...  --sink ...

``--source``/``--sink``/``--transform`` may be repeated. ``--engine`` selects the
Ibis backend (duckdb default), the lever that makes the same pipeline run on a
different dataframe implementation.
"""

from __future__ import annotations

import argparse

from .engine import Engine
from .etl import ETL, Sink, Source, Transformation
from .graph import Graph
from .registry import source_sink_schemes, transform_schemes
from .runner import run_nodes


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dfio",
        description="URI-based, engine-agnostic ETL (built on Ibis).",
    )
    parser.add_argument(
        "--source", action="append", default=[], metavar="URI",
        help=f"Source URI (repeatable). Schemes: {sorted(source_sink_schemes())}",
    )
    parser.add_argument(
        "--transform", action="append", default=[], metavar="URI",
        help=f"Transform URI (repeatable). Schemes: {sorted(transform_schemes())}",
    )
    parser.add_argument(
        "--sink", action="append", default=[], metavar="URI",
        help="Sink URI (repeatable).",
    )
    parser.add_argument(
        "--graph", metavar="PATH",
        help="Declarative graph file (.json/.yaml). Mutually exclusive with "
        "--source/--transform/--sink.",
    )
    parser.add_argument(
        "--engine", default="duckdb", help="Ibis backend (default: duckdb).",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    if args.graph:
        assert not (args.source or args.transform or args.sink), (
            "--graph cannot be combined with --source/--transform/--sink"
        )
        engine = Engine.from_config(args.engine)
        nodes = Graph.load(args.graph).compile()
        run_nodes(nodes, engine)
        print("Write successful")
        return
    etl = ETL(
        sources=[Source.parse(s) for s in args.source],
        sinks=[Sink.parse(s) for s in args.sink],
        transforms=[Transformation.parse(t) for t in args.transform],
        backend=args.engine,
    )
    etl.run()


if __name__ == "__main__":
    main()
