# JDBCParquetWriter [WebSite](http://manticore-projects.com/JDBCParquetWriter)

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/fcfaaa26ddf84063ad0fc23a70dcd7c2)](https://app.codacy.com/gh/manticore-projects/JDBCParquetWriter?utm_source=github.com&utm_medium=referral&utm_content=manticore-projects/JDBCParquetWriter&utm_campaign=Badge_Grade)

**JDBCParquetWriter** is a Java Library for writing [Apache Parquet](https://parquet.apache.org/) Files from JDBC Tables or ResultSets. It uses [Apache Hadoop](https://hadoop.apache.org/) and Parquet to translate the JDBC rows into the column based format.
The Parquet File can be imported into Column based Analytics Databases such as [ClickHouse](https://clickhouse.com/) or [DuckDB](https://duckdb.org/).

## Artifact

```xml
<dependency>
    <groupId>com.manticore-projects.jdbc</groupId>
    <artifactId>JDBCParquetWriter</artifactId>
    <version>1.2.0</version>
</dependency>
```

## Example

```sql
CREATE TABLE test.execution_ref (
    id_execution_ref         DECIMAL(9) NOT NULL
    , value_date             DATE       NOT NULL
    , posting_date           TIMESTAMP  NOT NULL
)
;

INSERT INTO TEST.EXECUTION_REF
    VALUES (1, {d '2021-01-06'}, {ts '2021-01-15 07:48:40.851'});
INSERT INTO TEST.EXECUTION_REF
    VALUES (2, {d '2021-01-12'}, {ts '2021-01-13 06:55:10.329'});
INSERT INTO TEST.EXECUTION_REF
    VALUES (3, {d '2021-01-13'}, {ts '2021-01-14 05:00:41.136'});
```


```java
String tableName = "execution_ref";
File file = File.createTempFile(tableName, ".parquet");

String sqlStr = "SELECT * FROM " + tableName;
try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sqlStr);) {
    JDBCParquetWriter.write(file, tableName, rs);
}
```

```text
are@archlinux ~/d/s/VBox (default) [1]> duckdb
v0.7.1 b00b93f0b1

D select * from read_parquet("/tmp/execution_ref8120677930150886139.parquet");
┌──────────────────┬────────────┬────────────────────────────┐
│ ID_EXECUTION_REF │ VALUE_DATE │        POSTING_DATE        │
│      int64       │    date    │  timestamp with time zone  │
├──────────────────┼────────────┼────────────────────────────┤
│                1 │ 2021-01-06 │ 2021-01-15 07:48:40.851+07 │
│                2 │ 2021-01-12 │ 2021-01-13 06:55:10.329+07 │
│                3 │ 2021-01-13 │ 2021-01-14 05:00:41.136+07 │
│                4 │ 2021-01-14 │ 2021-01-15 01:03:56.375+07 │
│                5 │ 2021-01-15 │ 2021-01-16 00:19:20.212+07 │
└──────────────────┴────────────┴────────────────────────────┘
```
