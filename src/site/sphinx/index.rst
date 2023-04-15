######################################
JDBC ResultSet To Parquet File Writer
######################################

.. toctree::
   :maxdepth: 2
   :hidden:

   usage
   changelog


**JDBCParquetWriter** is a Java Library for writing `Apache Parquet <https://parquet.apache.org/>`_ Files from JDBC Tables or ResultSets. It uses `Apache Hadoop <https://hadoop.apache.org/>`_ and Parquet to translate the JDBC rows into the column based format.
The Parquet File can be imported into Column based Analytics Databases such as `ClickHouse <https://clickhouse.com/>`_ or `DuckDB <https://duckdb.org/>`_.

Latest stable release: |JDBCPARQUETWRITER_STABLE_VERSION_LINK|

Development version: |JDBCPARQUETWRITER_SNAPSHOT_VERSION_LINK|

.. code-block:: Java
    :caption: Sample SQL Statement

    JDBCParquetWriter.write(file, tableName, resultSet);


*******************************
Features
*******************************

    * Table Schema derived from JDBC ResultSetMetaData
    * Support for Annotated Types:
        - Date
        - Time
        - Timestamp
        - BigDecimal
        - Decimal and Numeric, considering the Scale
    * Support for Nullable
    * Compression Support
    * Schema or Tables Bulk Export in parallel






