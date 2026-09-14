******************************
How to use it
******************************

Compile from Source Code
==============================

You will need to have ``JDK 17`` or later installed. The Gradle Toolchain of the build requests
``JDK 21``, which Gradle will provision automatically when it is not present.

.. tab:: Gradle

    .. code-block:: shell

        git clone https://github.com/manticore-projects/JDBCParquetWriter.git
        cd JDBCParquetWriter
        gradle build

.. tab:: Maven

    .. code-block:: shell

        git clone https://github.com/manticore-projects/JDBCParquetWriter.git
        cd JDBCParquetWriter
        mvn install


Build Dependencies
==============================

.. tab:: Maven Release

    .. code-block:: xml
        :substitutions:

        <dependency>
            <groupId>com.manticore-projects.jdbc</groupId>
            <artifactId>jdbcparquetwriter</artifactId>
            <version>|JDBCPARQUETWRITER_VERSION|</version>
        </dependency>

.. tab:: Maven Snapshot

    .. code-block:: xml
        :substitutions:

        <repositories>
            <repository>
                <id>central-snapshots</id>
                <snapshots>
                    <enabled>true</enabled>
                </snapshots>
                <url>https://central.sonatype.com/repository/maven-snapshots/</url>
            </repository>
        </repositories>
        <dependency>
            <groupId>com.manticore-projects.jdbc</groupId>
            <artifactId>jdbcparquetwriter</artifactId>
            <version>|JDBCPARQUETWRITER_SNAPSHOT_VERSION|</version>
        </dependency>

.. tab:: Gradle Stable

    .. code-block:: groovy
        :substitutions:

        repositories {
            mavenCentral()
        }

        dependencies {
            implementation 'com.manticore-projects.jdbc:jdbcparquetwriter:|JDBCPARQUETWRITER_VERSION|'
        }

.. tab:: Gradle Snapshot

    .. code-block:: groovy
        :substitutions:

        repositories {
            maven {
                url = uri('https://central.sonatype.com/repository/maven-snapshots/')
            }
        }

        dependencies {
            implementation 'com.manticore-projects.jdbc:jdbcparquetwriter:|JDBCPARQUETWRITER_SNAPSHOT_VERSION|'
        }

..  note::
    OSSRH (``oss.sonatype.org``) was retired in June 2025. Releases and Snapshots are served by the
    Sonatype Central Portal now.


Writing Parquet Files
==============================

Every entry point derives the Parquet Schema from the JDBC metadata and streams the rows into a
single local file. All of them return the tally of written rows.

From a ResultSet
------------------------------

.. code-block:: Java

    String tableName = "execution_ref";
    File file = File.createTempFile(tableName, ".parquet");

    String sqlStr = "SELECT * FROM test." + tableName;
    try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sqlStr)) {
        long writtenRows = JDBCParquetWriter.write(file, tableName, rs);
    }

From a Table
------------------------------

.. code-block:: Java

    long writtenRows = JDBCParquetWriter.write(file, "test.execution_ref", conn);

With a specific Compression Codec
----------------------------------

``SNAPPY`` is used when no codec is given.

.. code-block:: Java

    JDBCParquetWriter.write(file, tableName, rs, CompressionCodecName.ZSTD);

From a Query, returning the Import Statement
---------------------------------------------

.. code-block:: Java

    String importStr = JDBCParquetWriter.writeFileForQueryResult(
            folder, "SELECT * FROM test.execution_ref", "execution_ref",
            conn, JDBCParquetWriter.Dialect.DUCKDB, CompressionCodecName.SNAPPY);

    // INSERT INTO execution_ref SELECT * FROM read_parquet('/tmp/execution_ref.parquet');

``Dialect.CLICKHOUSE`` emits ``INSERT INTO ... SELECT * FROM file('...', Parquet);`` instead.

Every Table of a Query
------------------------------

``writeFilesForQueryTables()`` parses the statement with
`JSqlParser <https://github.com/JSQLParser/JSqlParser>`_, collects the source tables and exports
each of them into its own file.

.. code-block:: Java

    String importStr = JDBCParquetWriter.writeFilesForQueryTables(
            folder, complexQuery, conn,
            JDBCParquetWriter.Dialect.DUCKDB, CompressionCodecName.SNAPPY);

..  note::
    The target table of an ``INSERT``, ``UPDATE``, ``DELETE`` or ``MERGE`` is not a source of data
    and is therefore not exported. Use ``TableNamesFinder.getTargetTableName()`` to retrieve it.


Type Mapping
==============================

.. list-table::
    :header-rows: 1
    :widths: 45 20 35

    * - JDBC Type
      - Parquet Type
      - Logical Annotation
    * - ``BOOLEAN``
      - ``BOOLEAN``
      -
    * - ``TINYINT``, ``SMALLINT``, ``INTEGER``
      - ``INT32``
      -
    * - ``BIGINT``
      - ``INT64``
      -
    * - ``REAL``
      - ``FLOAT``
      -
    * - ``FLOAT``, ``DOUBLE``
      - ``DOUBLE``
      -
    * - ``CHAR``, ``VARCHAR``, ``CLOB`` and the ``N`` variants
      - ``BINARY``
      - ``String``
    * - ``BINARY``, ``VARBINARY``, ``BLOB``
      - ``BINARY``
      -
    * - ``DATE``
      - ``INT32``
      - ``Date``
    * - ``TIME``
      - ``INT32``
      - ``Time(MILLIS)``
    * - ``TIMESTAMP``
      - ``INT64``
      - ``Timestamp(MILLIS, UTC)``
    * - ``DECIMAL``, ``NUMERIC`` with Scale > 0 and Precision <= 18
      - ``INT64``
      - ``Decimal(p,s)``
    * - ``DECIMAL``, ``NUMERIC`` with Scale > 0 and Precision > 18
      - ``BINARY``
      - ``Decimal(p,s)``
    * - ``DECIMAL``, ``NUMERIC`` with Scale = 0 and Precision < 5
      - ``INT32``
      -
    * - ``DECIMAL``, ``NUMERIC`` with Scale = 0 and Precision >= 5
      - ``INT64``
      -

A column of any other type raises an ``IllegalArgumentException``. Columns reported as nullable
become ``optional`` fields, all others ``required``.

..  hint::
    Oracle reports a Scale of ``-127`` when the Scale of a ``NUMBER`` was left unspecified. Such a
    column is treated as ``DECIMAL(38,10)``.