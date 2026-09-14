.. meta::
   :description: Java Software Library for writing Parquet Files from queries or result sets
   :keywords: parquet file, columnar database, result set, query, output, writer, duckdb, clickhouse

######################################
JDBC ResultSet To Parquet File Writer
######################################

.. image:: _static/logo.svg
    :alt: JDBCParquetWriter
    :width: 560px
    :align: center

.. toctree::
    :maxdepth: 2
    :hidden:

    usage
    Java API <javadoc.rst>
    Changelog <changelog.rst>


**JDBCParquetWriter** is a Java Library for writing `Apache Parquet <https://parquet.apache.org/>`_ Files from JDBC Tables or ResultSets. It uses `Apache Hadoop <https://hadoop.apache.org/>`_ and Parquet to translate the JDBC rows into the column based format.
The Parquet File can be imported into Column based Analytics Databases such as `ClickHouse <https://clickhouse.com/>`_ or `DuckDB <https://duckdb.org/>`_.

Latest stable release: |JDBCPARQUETWRITER_STABLE_VERSION_LINK|

Development version: |JDBCPARQUETWRITER_SNAPSHOT_VERSION_LINK|

.. code-block:: Java
    :caption: Write a ResultSet into a Parquet File

    String tableName = "execution_ref";
    File file = File.createTempFile(tableName, ".parquet");

    String sqlStr = "SELECT * FROM test." + tableName;
    try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sqlStr)) {
        long writtenRows = JDBCParquetWriter.write(file, tableName, rs);
    }

.. code-block:: text
    :caption: Read it back with DuckDB

    D SELECT * FROM read_parquet('/tmp/execution_ref.parquet');
    ┌──────────────────┬────────────┬────────────────────────────┬───────────────┐
    │ ID_EXECUTION_REF │ VALUE_DATE │        POSTING_DATE        │    AMOUNT     │
    │      int64       │    date    │  timestamp with time zone  │ decimal(23,5) │
    ├──────────────────┼────────────┼────────────────────────────┼───────────────┤
    │                1 │ 2021-01-06 │ 2021-01-15 07:48:40.851+07 │     100.22000 │
    │                2 │ 2021-01-12 │ 2021-01-13 06:55:10.329+07 │      75.30000 │
    │                3 │ 2021-01-13 │ 2021-01-14 05:00:41.136+07 │          NULL │
    └──────────────────┴────────────┴────────────────────────────┴───────────────┘

..  note::
    The file name is a *string literal* and belongs into single quotes. Double quotes would make
    DuckDB read it as an identifier.


*******************************
Features
*******************************

    * Table Schema derived from the JDBC ``ResultSetMetaData``, including Nullability
    * Support for Annotated Types:
        - Date
        - Time
        - Timestamp
        - Decimal and Numeric, honouring Precision and Scale
        - Oracle's unspecified ``NUMBER`` Scale
    * Support for Character and Binary Large Objects
    * Row by row streaming, the source table never has to fit into memory
    * Compression Support, SNAPPY by default
    * Schema or Tables Bulk Export
    * Generates the matching ``INSERT ... SELECT * FROM read_parquet(...)`` Import Statement for
      DuckDB and ClickHouse