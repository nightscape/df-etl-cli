# DataFrame IO (Python)

A Python port of the Scala [`dataframe-io`](../README.md) library. DataFrame IO
allows you to

* read data from various **sources**
* transform it using various **transformers**
* write data to various **sinks**

by specifying source, transform and sink URIs.

Unlike the original, which is built on Apache Spark, this port is built on
[Ibis](https://ibis-project.org/) — a portable dataframe API. Ibis is the
dataframe abstraction: the same pipeline runs on any Ibis backend (duckdb,
polars, pyspark, postgres, …) by changing a single `--engine` flag, with no
change to the pipeline itself. The default backend is duckdb.

The URI schema is

```
protocol://host/path?queryParam1=value1&queryParam2=value2
```

The protocol decides which source or sink is actually used. Currently, the
following options for sources and sinks are available:

* [Console](#console) (`console://`)
* [Values](#values) (`values://`)
* [Text](#text) (CSV, TSV) (`text://`)
* [Parquet](#parquet) (`parquet://`)

For transformations, the following options are available:

* [Identity](#identity) (`identity://`)
* [SQL](#sql) (`sql://`)
* [SQL-file](#sql-file) (`sql-file://`)
* [Flatten](#flatten) (`flatten://`)
* [Flatten and explode](#flatten-and-explode) (`flatten-explode://`)

> Delta, Excel, Hive, Kafka, Solr, Avro and streaming sources from the Scala
> version are **not yet ported**.

## Installation

```sh
uv venv --python 3.12
uv pip install -e '.[dev]'          # add ,polars or ,pyspark for those engines
```

## Running

### CLI

The package installs a `dfio` console script that wires `--source`,
`--transform`, and `--sink` URIs into a pipeline. Each option may be repeated.

```sh
dfio --source 'data+text:///path/to/in.csv' \
     --transform 'data+out+sql:///SELECT%20*%20FROM%20data%20WHERE%20n%3E1' \
     --sink 'out+parquet:///path/to/out.parquet'
```

Select a different engine — the rest of the pipeline is unchanged:

```sh
dfio --engine polars --source ... --sink ...
```

`dfio --help` lists the available schemes.

### Python API

```python
from dfio import ETL, Source, Transformation, Sink

ETL(
    sources=[Source.parse("data+text:///path/to/in.csv")],
    transforms=[Transformation.parse("data+out+sql:///SELECT * FROM data WHERE n > 1")],
    sinks=[Sink.parse("out+parquet:///path/to/out.parquet")],
    backend="duckdb",
).run()
```

## Source and Sink URI schemas

The URI schema for sources and sinks generally looks like this

```
dfToReadInto+sourceType://some-host/some-path?additional=parameters
sourceType://some-host/some-path?additional=parameters

dfToPersist+sinkType://some-host/some-path?additional=parameters
sinkType://some-host/some-path?additional=parameters
```

The possible options for `sourceType` and `sinkType` are listed below.
The `dfToReadInto` is used to save the result of reading the source. If not
specified, it defaults to `"source"`.
The `dfToPersist` is the name of the DataFrame that should be persisted to the
sink. If not specified, it defaults to `"sink"`.

Both `dfToReadInto` and `dfToPersist` are optional. Because they live in the URI
*scheme*, they must be valid scheme characters: use hyphens, not underscores
(hyphens are normalized to underscores internally so names are valid SQL
identifiers, e.g. `my-data` becomes the table `my_data`).

### Console
```
console://anything
```
The source returns an empty DataFrame.
The sink prints an excerpt of the DataFrame to the console.

### Values
```
values:///?header=foo:int,bar:string&values=1,a;2,b
```
The source returns a DataFrame with column names and types specified in `header`
and values specified in `values` (rows separated by `;`, cells by `,`).
Supported types are `int`, `long`, `double`; anything else is `string`.
The sink prints an excerpt of the DataFrame to the console.

### Text
```
text:///path/to/some.csv
```
Reads/writes CSV or TSV files, with the delimiter determined by file extension
(`.csv` → `,`, `.tsv` → tab). `?header=` defaults to `true`. CSV IO is routed
through Apache Arrow so behaviour is identical across every engine.

### Parquet
```
parquet:///path/to/file.parquet
```
Reads/writes Parquet at the given path.

## Transformation URI Schemas

The URI schema for transformations generally looks like this

```
sourceName+sinkName+transformationType://some-host/some-path?additional=parameters
```

The `sourceName` is the previously named intermediate DataFrame used as input.
By default it is `"source"`. The `sinkName` registers the result under a name;
by default `"sink"`. Both can be specified or omitted:

```
transformationType://                       # both default to "source"/"sink"
sourceName+transformationType://             # only sourceName given
sourceName+sinkName+transformationType://    # both given
```

### Identity
```
sourceName+sinkName+identity:///
```
Renames a DataFrame from `sourceName` to `sinkName` (passthrough).

### SQL
```
sql:///SELECT%20foo%20AS%20bar%20FROM%20sourceName
```
Applies inline SQL to its input. The SQL must be URL encoded (a space is `%20`)
and follow the triple slash `sql:///`. The query runs against the engine's
catalog, so it may reference any previously registered named DataFrame by name.
This is only convenient for short queries.

The SQL is parsed in a fixed dialect (`duckdb` by default) and transpiled by Ibis
to whichever engine runs it, so the *same* query means the same thing on every
backend rather than being reinterpreted per engine. Override the input dialect
with `?dialect=` (e.g. `sql:///<encoded>?dialect=postgres`).

### SQL-File
```
sql-file:///path/to/query.sql
```
Applies SQL read from a file to its input. The referenced tables must have been
registered in a previous step.

### Flatten
```
sourceName+sinkName+flatten:///
```
Recursively unpacks nested struct columns into `parent_child` columns.

### Flatten and explode
```
sourceName+sinkName+flatten-explode:///
```
Recursively unpacks struct columns **and** explodes array columns into rows.

## Extending with plugins

External packages register new schemes via entry points, the Python analogue of
the Scala `ServiceLoader`:

```toml
[project.entry-points."dfio.sources"]
my-scheme = "my_package:MyUriParser"

[project.entry-points."dfio.transforms"]
my-transform = "my_package:MyTransformerParser"
```

## Tests

Tests are property-based, using [Hypothesis](https://hypothesis.readthedocs.io/):

```sh
.venv/bin/pytest | tee test-output.txt
```
