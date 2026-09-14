<div align="center">

<img src="src/site/sphinx/_static/logo.svg" alt="JDBCParquetWriter" width="560">

**Write [Apache Parquet](https://parquet.apache.org/) files straight from a JDBC `ResultSet` — no Spark, no Avro, no intermediate CSV.**

[![Maven Central](https://img.shields.io/maven-central/v/com.manticore-projects.jdbc/jdbcparquetwriter?style=flat-square&logo=apachemaven&logoColor=white&label=Maven%20Central&color=030146)](https://central.sonatype.com/artifact/com.manticore-projects.jdbc/jdbcparquetwriter)
[![Build](https://img.shields.io/github/actions/workflow/status/manticore-projects/JDBCParquetWriter/gradle.yml?branch=main&style=flat-square&logo=githubactions&logoColor=white&label=build)](https://github.com/manticore-projects/JDBCParquetWriter/actions/workflows/gradle.yml)
[![CodeQL](https://img.shields.io/github/actions/workflow/status/manticore-projects/JDBCParquetWriter/codeql.yml?branch=main&style=flat-square&logo=github&label=CodeQL)](https://github.com/manticore-projects/JDBCParquetWriter/actions/workflows/codeql.yml)
[![Codacy](https://img.shields.io/codacy/grade/fcfaaa26ddf84063ad0fc23a70dcd7c2?style=flat-square&logo=codacy&label=code%20quality)](https://app.codacy.com/gh/manticore-projects/JDBCParquetWriter/dashboard)
[![Java](https://img.shields.io/badge/Java-17%2B-030146?style=flat-square&logo=openjdk&logoColor=white)](https://adoptium.net/)
[![License](https://img.shields.io/badge/License-GPL--3.0-ff420e?style=flat-square)](LICENSE)

[Website](https://manticore-projects.com/JDBCParquetWriter) ·
[Usage](https://manticore-projects.com/JDBCParquetWriter/usage.html) ·
[Java API](https://manticore-projects.com/JDBCParquetWriter/javadoc.html) ·
[Changelog](https://manticore-projects.com/JDBCParquetWriter/changelog.html)

</div>

---

## Why

Analytics engines like [DuckDB](https://duckdb.org/) and [ClickHouse](https://clickhouse.com/) read
Parquet natively and fast. Getting data *out* of an operational RDBMS and *into* Parquet normally
means a Spark job, an Avro schema or a lossy CSV round trip.

**JDBCParquetWriter** does it in one call. It derives the Parquet schema from
`ResultSetMetaData`, streams the rows through Parquet's `ParquetWriter` and writes a single local
file — preserving nullability, `DECIMAL` precision and scale, dates and timestamps.

- Schema derived automatically from the JDBC metadata, nullability included
- Exact `DECIMAL` transport: `INT64` up to 18 digits of precision, `BINARY` beyond it
- Handles Oracle's unspecified `NUMBER` scale (`-127`)
- Row-by-row streaming, so the source table never has to fit in memory
- SNAPPY by default, any `CompressionCodecName` on request
- Generates the matching `INSERT … SELECT * FROM read_parquet(…)` import statement

## Install

```xml
<dependency>
    <groupId>com.manticore-projects.jdbc</groupId>
    <artifactId>jdbcparquetwriter</artifactId>
    <version>[1.3,)</version>
</dependency>
```

```groovy
implementation 'com.manticore-projects.jdbc:jdbcparquetwriter:+'
```

Requires Java 17 or later. Snapshots are published to
`https://central.sonatype.com/repository/maven-snapshots/`.

## Usage

Given an ordinary table:

```sql
CREATE TABLE test.execution_ref (
    id_execution_ref         DECIMAL(9) NOT NULL
    , value_date             DATE       NOT NULL
    , posting_date           TIMESTAMP  NOT NULL
    , amount                 DECIMAL(23,5)  NULL
);

INSERT INTO test.execution_ref VALUES (1, {d '2021-01-06'}, {ts '2021-01-15 07:48:40.851'}, 100.22);
INSERT INTO test.execution_ref VALUES (2, {d '2021-01-12'}, {ts '2021-01-13 06:55:10.329'}, 75.30);
INSERT INTO test.execution_ref VALUES (3, {d '2021-01-13'}, {ts '2021-01-14 05:00:41.136'}, NULL);
```

### Write a ResultSet

```java
String tableName = "execution_ref";
File file = File.createTempFile(tableName, ".parquet");

String sqlStr = "SELECT * FROM test." + tableName;
try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sqlStr)) {
    long rows = JDBCParquetWriter.write(file, tableName, rs);
}
```

### Write a whole table

```java
long rows = JDBCParquetWriter.write(file, "test.execution_ref", conn);
```

### Choose a codec

```java
JDBCParquetWriter.write(file, tableName, rs, CompressionCodecName.ZSTD);
```

### Write a query and get the import statement back

```java
String importStr = JDBCParquetWriter.writeFileForQueryResult(
        folder, "SELECT * FROM test.execution_ref", "execution_ref",
        conn, JDBCParquetWriter.Dialect.DUCKDB, CompressionCodecName.SNAPPY);

// INSERT INTO execution_ref SELECT * FROM read_parquet('/tmp/execution_ref.parquet');
```

### Export every table a query reads

`writeFilesForQueryTables` parses the statement with
[JSqlParser](https://github.com/JSQLParser/JSqlParser), finds the source tables and dumps each one:

```java
String importStr = JDBCParquetWriter.writeFilesForQueryTables(
        folder, complexQuery, conn,
        JDBCParquetWriter.Dialect.DUCKDB, CompressionCodecName.SNAPPY);
```

## Reading it back

```text
$ duckdb
D SELECT * FROM read_parquet('/tmp/execution_ref.parquet');
┌──────────────────┬────────────┬────────────────────────────┬───────────────┐
│ ID_EXECUTION_REF │ VALUE_DATE │        POSTING_DATE        │    AMOUNT     │
│      int64       │    date    │  timestamp with time zone  │ decimal(23,5) │
├──────────────────┼────────────┼────────────────────────────┼───────────────┤
│                1 │ 2021-01-06 │ 2021-01-15 07:48:40.851+07 │     100.22000 │
│                2 │ 2021-01-12 │ 2021-01-13 06:55:10.329+07 │      75.30000 │
│                3 │ 2021-01-13 │ 2021-01-14 05:00:41.136+07 │          NULL │
└──────────────────┴────────────┴────────────────────────────┴───────────────┘
```

> **Note** — the path is a *string literal*, so it belongs in single quotes. Double quotes make
> DuckDB read it as an identifier.

## Type mapping

| JDBC type | Parquet type | Logical annotation |
|---|---|---|
| `BOOLEAN` | `BOOLEAN` | — |
| `TINYINT`, `SMALLINT`, `INTEGER` | `INT32` | — |
| `BIGINT` | `INT64` | — |
| `REAL` | `FLOAT` | — |
| `FLOAT`, `DOUBLE` | `DOUBLE` | — |
| `CHAR`, `VARCHAR`, `CLOB` (and the `N` variants) | `BINARY` | `String` |
| `BINARY`, `VARBINARY`, `BLOB` | `BINARY` | — |
| `DATE` | `INT32` | `Date` |
| `TIME` | `INT32` | `Time(MILLIS)` |
| `TIMESTAMP` | `INT64` | `Timestamp(MILLIS, UTC)` |
| `DECIMAL`/`NUMERIC`, scale > 0, precision ≤ 18 | `INT64` | `Decimal(p,s)` |
| `DECIMAL`/`NUMERIC`, scale > 0, precision > 18 | `BINARY` | `Decimal(p,s)` |
| `DECIMAL`/`NUMERIC`, scale = 0, precision < 5 | `INT32` | — |
| `DECIMAL`/`NUMERIC`, scale = 0, precision ≥ 5 | `INT64` | — |

A column of any other type raises `IllegalArgumentException`. Columns reported as nullable become
`optional`, the rest `required`.

## Building

```shell
gradle build
```

The build runs Checkstyle, PMD, SpotBugs, Spotless and JaCoCo. JMH benchmarks comparing H2 and
DuckDB live in `PerformanceTest`; the `SAMPLE_SIZE` environment variable controls the generated
portfolio size — be careful, 100 million rows produce a 20 GB H2 database.

```shell
gradle jmh
```

## License

GPL-3.0. See [LICENSE](LICENSE).

---

<div align="center">
Part of the <a href="https://manticore-projects.com">Manticore Projects</a> toolchain, alongside
<a href="https://github.com/JSQLParser/JSqlParser">JSqlParser</a> and
<a href="https://github.com/manticore-projects/MJdbcUtils">MJdbcUtils</a>.
</div>
