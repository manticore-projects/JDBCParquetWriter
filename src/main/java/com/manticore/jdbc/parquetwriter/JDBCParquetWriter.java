/**
 * Copyright (C) 2023 Andreas Reichel <andreas@manticore-projects.com>
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the
 * GNU Lesser General Public License as published by the Free Software Foundation; either version 3
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along with this program;
 * if not, write to the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */
package com.manticore.jdbc.parquetwriter;

import net.sf.jsqlparser.JSQLParserException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.logging.Logger;

/**
 * The JDBC parquet writer class.
 */
public final class JDBCParquetWriter {
    /**
     * The constant LOGGER.
     */
    public static final Logger LOGGER = Logger.getLogger(JDBCParquetWriter.class.getName());

    /**
     * Oracle reports -127 as the scale of a NUMBER/FLOAT/DECIMAL/DOUBLE column when the scale was
     * left unspecified, see OracleResultSetMetaData#isVariableScale(int).
     */
    private static final int ORACLE_VARIABLE_SCALE = -127;

    /** Precision and scale assumed for a column of Oracle's unspecified scale. */
    private static final int VARIABLE_PRECISION = 38;
    private static final int VARIABLE_SCALE = 10;

    /** Largest decimal precision that still fits into a signed 64 bit unscaled value. */
    private static final int MAX_LONG_PRECISION = 18;

    /** Precision below which a scale-less decimal still fits into a signed 32 bit value. */
    private static final int MAX_INT_PRECISION = 5;

    /** Width of the two's complement byte array written for BINARY backed decimals. */
    private static final int DECIMAL_BINARY_BYTES = 16;

    private JDBCParquetWriter() {
        // utility class, not meant to be instantiated
    }

    /**
     * The enum Dialect.
     */
    public enum Dialect {
        /**
         * Duckdb dialect.
         */
        DUCKDB,
        /**
         * Clickhouse dialect.
         */
        CLICKHOUSE
    }

    /**
     * Writes a query into a parquet file and returns an INSERT statement string for importing this
     * file into a table of the given dialect.
     *
     * @param folder the folder for writing the parquet file to
     * @param qryStr the query to execute
     * @param targetTableName the target table name
     * @param conn the JDBC connection for executing the query
     * @param dialect the dialect for the INSERT statement
     * @param compressionCodecName the compression codec name
     * @return the INSERT statement text
     * @throws SQLException any exception during query execution
     * @throws IOException any exception while writing the file
     */
    public static String writeFileForQueryResult(File folder, String qryStr, String targetTableName,
            Connection conn, Dialect dialect, CompressionCodecName compressionCodecName)
            throws SQLException, IOException {
        File file = new File(folder, targetTableName + ".parquet");
        try (
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(qryStr)) {
            write(file, targetTableName, rs, compressionCodecName);
            LOGGER.info("Wrote parquet file: " + file.getAbsolutePath());
        }

        String importQryStr = getImportStatement(dialect, targetTableName, file);
        LOGGER.info("Import statement: " + importQryStr);

        return importQryStr;
    }

    /**
     * Builds the dialect specific statement importing a parquet file into an existing table.
     *
     * @param dialect the target dialect
     * @param targetTableName the target table name
     * @param file the parquet file to read
     * @return the INSERT statement text
     */
    public static String getImportStatement(Dialect dialect, String targetTableName, File file) {
        // a file system path is a string literal in both dialects, so it belongs into single
        // quotes; double quotes would make DuckDB read it as an identifier
        String path = quote(file.getAbsolutePath());

        switch (dialect) {
            case CLICKHOUSE:
                return "INSERT INTO " + targetTableName
                        + " SELECT * FROM file(" + path + ", Parquet);";
            case DUCKDB:
            default:
                return "INSERT INTO " + targetTableName
                        + " SELECT * FROM read_parquet(" + path + ");";
        }
    }

    private static String quote(String s) {
        return "'" + s.replace("'", "''") + "'";
    }

    /**
     * Write all tables used in a query into parquet files and returns an INSERT statement string
     * for importing all those files into tables of the given dialect.
     *
     * @param folder the folder for writing the parquet file to
     * @param qryStr the query definition with the tables
     * @param conn the JDBC connection for executing the query
     * @param dialect the dialect for the INSERT statement
     * @param compressionCodecName the compression codec name
     * @return the INSERT statement text
     * @throws SQLException any exception during query execution
     * @throws IOException any exception while writing the files
     * @throws JSQLParserException when the query can not be parsed
     */
    public static String writeFilesForQueryTables(File folder, String qryStr, Connection conn,
            Dialect dialect, CompressionCodecName compressionCodecName)
            throws SQLException, IOException, JSQLParserException {
        StringBuilder importQryStr = new StringBuilder();

        TableNamesFinder finder = new TableNamesFinder(qryStr);
        for (String tableName : finder.getSourceTableNames()) {
            LOGGER.info("Create parquet file for table: " + tableName);

            importQryStr.append("\n").append(writeFileForQueryResult(
                    folder, "SELECT * FROM " + tableName, tableName, conn, dialect,
                    compressionCodecName));
        }
        return importQryStr.toString();
    }

    /**
     * Writes a source table into a ParquetFile using SNAPPY for compression
     *
     * @param file the destination File
     * @param tableName the source table name
     * @param conn the source JDBC connection for accessing the source table
     * @return the tally of written rows
     * @throws SQLException any exception from query execution
     * @throws IOException any exception while writing the file
     */
    public static long write(File file, String tableName, Connection conn)
            throws SQLException, IOException {
        return write(file, tableName, conn, CompressionCodecName.SNAPPY);
    }

    /**
     * Writes a source table into a ParquetFile
     *
     * @param file the destination File
     * @param tableName the source table name
     * @param conn the source JDBC connection for accessing the source table
     * @param compressionCodecName the compression codec name
     * @return the tally of written rows
     * @throws SQLException any exception from query execution
     * @throws IOException any exception while writing the file
     */
    public static long write(File file, String tableName, Connection conn,
            CompressionCodecName compressionCodecName) throws SQLException, IOException {
        long writtenRows;
        String qryStr = "SELECT * FROM " + tableName;
        try (
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(qryStr)) {
            writtenRows = write(file, tableName, rs, compressionCodecName);
            LOGGER.info("Wrote parquet file: " + file.getAbsolutePath());
        }
        return writtenRows;
    }

    /**
     * Writes a ResultSet into a ParquetFile using SNAPPY for compression
     *
     * @param f the destination File
     * @param tableName the destination table name
     * @param rs the ResultSet of data
     * @return the tally of written rows
     * @throws SQLException any exception from query execution
     * @throws IOException any exception while writing the file
     */
    public static long write(File f, String tableName, ResultSet rs)
            throws SQLException, IOException {
        return write(f, tableName, rs, CompressionCodecName.SNAPPY);
    }

    /**
     * Writes a ResultSet into a ParquetFile.
     *
     * @param f the destination File
     * @param tableName the destination table name
     * @param rs the ResultSet of data
     * @param compressionCodecName the compression codec name
     * @return the tally of written rows
     * @throws SQLException any exception from query execution
     * @throws IOException any exception while writing the file
     */
    public static long write(File f, String tableName, ResultSet rs,
            CompressionCodecName compressionCodecName) throws SQLException, IOException {
        long writtenRows = 0L;

        // needed for org.apache.hadoop.util.Shell; getParent() is null for a bare file name
        File parent = f.getAbsoluteFile().getParentFile();
        if (parent != null) {
            System.setProperty("hadoop.home.dir", parent.getAbsolutePath());
        }

        Path outputPath = new Path(f.toURI());

        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        // SELECT FROM tablename is valid, but does not return any columns
        if (columnCount == 0) {
            throw new SQLException("ResultSet without any Columns. Please verify your Query.");
        }

        MessageType schema = getParquetSchemaFromResultSet(tableName, metaData);

        Configuration configuration = new Configuration();
        GroupWriteSupport.setSchema(schema, configuration);

        ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(outputPath)
                .withConf(configuration)
                .withType(schema)
                .withCompressionCodec(compressionCodecName)
                .withDictionaryEncoding(true)
                .withValidation(false)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE);

        try (ParquetWriter<Group> writer = builder.build()) {
            while (rs.next()) {
                Group group = new SimpleGroup(schema);
                for (int i = 1; i <= columnCount; i++) {
                    writeColumn(group, rs, metaData, i);
                }
                writer.write(group);
                writtenRows++;
            }
        }
        return writtenRows;
    }

    // ------------------------------------------------------------------------------------------
    // writing a single value
    // ------------------------------------------------------------------------------------------

    private static void writeColumn(Group group, ResultSet rs, ResultSetMetaData metaData, int i)
            throws SQLException {
        String columnName = metaData.getColumnName(i);
        int columnType = metaData.getColumnType(i);

        switch (columnType) {
            case java.sql.Types.BOOLEAN:
                boolean booleanValue = rs.getBoolean(i);
                if (!rs.wasNull()) {
                    group.add(columnName, booleanValue);
                }
                break;
            case java.sql.Types.BIGINT:
                long longValue = rs.getLong(i);
                if (!rs.wasNull()) {
                    group.add(columnName, longValue);
                }
                break;
            case java.sql.Types.INTEGER:
            case java.sql.Types.SMALLINT:
            case java.sql.Types.TINYINT:
                int intValue = rs.getInt(i);
                if (!rs.wasNull()) {
                    group.add(columnName, intValue);
                }
                break;
            case java.sql.Types.REAL:
                float floatValue = rs.getFloat(i);
                if (!rs.wasNull()) {
                    group.add(columnName, floatValue);
                }
                break;
            case java.sql.Types.FLOAT:
            case java.sql.Types.DOUBLE:
                double doubleValue = rs.getDouble(i);
                if (!rs.wasNull()) {
                    group.add(columnName, doubleValue);
                }
                break;
            case java.sql.Types.DECIMAL:
            case java.sql.Types.NUMERIC:
                writeDecimalColumn(group, rs, metaData, i);
                break;
            default:
                writeNonNumericColumn(group, rs, columnName, columnType, i);
        }
    }

    private static void writeNonNumericColumn(Group group, ResultSet rs, String columnName,
            int columnType, int i) throws SQLException {
        switch (columnType) {
            case java.sql.Types.VARCHAR:
            case java.sql.Types.NVARCHAR:
            case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.LONGNVARCHAR:
            case java.sql.Types.CHAR:
            case java.sql.Types.NCHAR:
            case java.sql.Types.CLOB:
            case java.sql.Types.NCLOB:
                String s = rs.getString(i);
                if (!rs.wasNull()) {
                    group.add(columnName, s);
                }
                break;
            case java.sql.Types.BLOB:
            case java.sql.Types.BINARY:
            case java.sql.Types.VARBINARY:
            case java.sql.Types.LONGVARBINARY:
                // was missing entirely: the schema accepted BLOB/BINARY columns, but writing one
                // fell through to the "Unsupported SQL type" default below
                byte[] bytes = rs.getBytes(i);
                if (!rs.wasNull()) {
                    group.add(columnName, Binary.fromConstantByteArray(bytes));
                }
                break;
            case java.sql.Types.DATE:
                Date d = rs.getDate(i);
                if (!rs.wasNull()) {
                    group.add(columnName, (int) d.toLocalDate().toEpochDay());
                }
                break;
            case java.sql.Types.TIME:
                Time t = rs.getTime(i);
                if (!rs.wasNull()) {
                    // TIME(MILLIS) is milliseconds since midnight, not since the epoch
                    group.add(columnName,
                            (int) (t.toLocalTime().toNanoOfDay() / 1_000_000L));
                }
                break;
            case java.sql.Types.TIMESTAMP:
                Timestamp ts = rs.getTimestamp(i);
                if (!rs.wasNull()) {
                    group.add(columnName, ts.getTime());
                }
                break;
            default:
                throw new IllegalArgumentException("Unsupported SQL type: " + columnType);
        }
    }

    private static void writeDecimalColumn(Group group, ResultSet rs, ResultSetMetaData metaData,
            int i) throws SQLException {
        BigDecimal decimal = rs.getBigDecimal(i);
        if (rs.wasNull()) {
            return;
        }

        String columnName = metaData.getColumnName(i);
        boolean variableScale = metaData.getScale(i) == ORACLE_VARIABLE_SCALE;
        int precision = variableScale ? VARIABLE_PRECISION : metaData.getPrecision(i);
        int scale = variableScale ? VARIABLE_SCALE : metaData.getScale(i);

        if (scale > 0 && precision <= MAX_LONG_PRECISION) {
            // align the value with the declared scale first; a driver is free to return a
            // BigDecimal of a different scale and the unscaled value would then be off by
            // a factor of ten per missing digit
            group.add(columnName,
                    decimal.setScale(scale, RoundingMode.HALF_EVEN).unscaledValue().longValue());
        } else if (scale > 0) {
            group.add(columnName, toDecimalBinary(decimal, scale));
        } else if (precision < MAX_INT_PRECISION) {
            group.add(columnName, decimal.intValue());
        } else {
            group.add(columnName, decimal.longValue());
        }
    }

    /** The unscaled value as a big endian two's complement byte array, sign extended. */
    private static Binary toDecimalBinary(BigDecimal decimal, int scale) {
        byte[] unscaledBytes = decimal
                .setScale(scale, RoundingMode.HALF_EVEN)
                .unscaledValue()
                .toByteArray();

        if (unscaledBytes.length >= DECIMAL_BINARY_BYTES) {
            // already at or beyond the padding width; toByteArray() is the minimal two's
            // complement representation and stays valid at any length
            return Binary.fromConstantByteArray(unscaledBytes);
        }

        byte[] paddedBytes = new byte[DECIMAL_BINARY_BYTES];
        byte signByte = unscaledBytes[0] < 0 ? (byte) 0xFF : 0x00;
        Arrays.fill(paddedBytes, 0, DECIMAL_BINARY_BYTES - unscaledBytes.length, signByte);
        System.arraycopy(unscaledBytes, 0, paddedBytes,
                DECIMAL_BINARY_BYTES - unscaledBytes.length, unscaledBytes.length);

        return Binary.fromConstantByteArray(paddedBytes);
    }

    // ------------------------------------------------------------------------------------------
    // deriving the schema
    // ------------------------------------------------------------------------------------------

    /**
     * Derive the parquet schema from a JDBC result set.
     *
     * @param tableName the table name
     * @param metadata the JDBC metadata
     * @return the parquet schema from result set
     * @throws SQLException any exception from query execution
     */
    public static MessageType getParquetSchemaFromResultSet(String tableName,
            ResultSetMetaData metadata) throws SQLException {
        int columnCount = metadata.getColumnCount();

        Types.MessageTypeBuilder builder = Types.buildMessage();
        for (int i = 1; i <= columnCount; i++) {
            builder.addField(getParquetType(metadata, i));
        }
        return builder.named(tableName);
    }

    private static PrimitiveType getParquetType(ResultSetMetaData metadata, int i)
            throws SQLException {
        String columnName = metadata.getColumnName(i);
        int columnType = metadata.getColumnType(i);
        int nullable = metadata.isNullable(i);

        switch (columnType) {
            case java.sql.Types.BOOLEAN:
                return primitive(PrimitiveTypeName.BOOLEAN, nullable).named(columnName);
            case java.sql.Types.BIGINT:
                return primitive(PrimitiveTypeName.INT64, nullable).named(columnName);
            case java.sql.Types.INTEGER:
            case java.sql.Types.SMALLINT:
            case java.sql.Types.TINYINT:
                return primitive(PrimitiveTypeName.INT32, nullable).named(columnName);
            case java.sql.Types.REAL:
                return primitive(PrimitiveTypeName.FLOAT, nullable).named(columnName);
            case java.sql.Types.FLOAT:
            case java.sql.Types.DOUBLE:
                // JDBC defines FLOAT as double precision, mapping it to a 4 byte float loses digits
                return primitive(PrimitiveTypeName.DOUBLE, nullable).named(columnName);
            case java.sql.Types.DECIMAL:
            case java.sql.Types.NUMERIC:
                return getDecimalParquetType(columnName, metadata.getPrecision(i),
                        metadata.getScale(i), nullable);
            default:
                return getNonNumericParquetType(columnName, columnType, nullable);
        }
    }

    private static PrimitiveType getNonNumericParquetType(String columnName, int columnType,
            int nullable) {
        switch (columnType) {
            case java.sql.Types.VARCHAR:
            case java.sql.Types.NVARCHAR:
            case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.LONGNVARCHAR:
            case java.sql.Types.CHAR:
            case java.sql.Types.NCHAR:
            case java.sql.Types.CLOB:
            case java.sql.Types.NCLOB:
                // CLOB/NCLOB are character data and are written through getString(), so they
                // carry the string annotation just like VARCHAR
                return primitive(PrimitiveTypeName.BINARY, nullable)
                        .as(LogicalTypeAnnotation.stringType()).named(columnName);
            case java.sql.Types.BLOB:
            case java.sql.Types.BINARY:
            case java.sql.Types.VARBINARY:
            case java.sql.Types.LONGVARBINARY:
                return primitive(PrimitiveTypeName.BINARY, nullable).named(columnName);
            case java.sql.Types.DATE:
                return primitive(PrimitiveTypeName.INT32, nullable)
                        .as(LogicalTypeAnnotation.dateType()).named(columnName);
            case java.sql.Types.TIME:
                // the Parquet spec requires INT32 for TIME(MILLIS); INT64 is only valid for
                // MICROS and NANOS. The value is a local time of day, so isAdjustedToUTC is false
                return primitive(PrimitiveTypeName.INT32, nullable)
                        .as(LogicalTypeAnnotation.timeType(false,
                                LogicalTypeAnnotation.TimeUnit.MILLIS))
                        .named(columnName);
            case java.sql.Types.TIMESTAMP:
                return primitive(PrimitiveTypeName.INT64, nullable)
                        .as(LogicalTypeAnnotation.timestampType(true,
                                LogicalTypeAnnotation.TimeUnit.MILLIS))
                        .named(columnName);
            default:
                throw new IllegalArgumentException("Unsupported SQL type: " + columnType);
        }
    }

    private static PrimitiveType getDecimalParquetType(String columnName, int declaredPrecision,
            int declaredScale, int nullable) {
        boolean variableScale = declaredScale == ORACLE_VARIABLE_SCALE;
        int precision = variableScale ? VARIABLE_PRECISION : declaredPrecision;
        int scale = variableScale ? VARIABLE_SCALE : declaredScale;

        // a precision of up to 18 digits can be transported as the unscaled value in a LONG,
        // anything wider needs the unscaled value's bytes in a BINARY
        if (scale > 0 && precision <= MAX_LONG_PRECISION) {
            return primitive(PrimitiveTypeName.INT64, nullable)
                    .as(LogicalTypeAnnotation.decimalType(scale, precision)).named(columnName);
        } else if (scale > 0) {
            return primitive(PrimitiveTypeName.BINARY, nullable)
                    .as(LogicalTypeAnnotation.decimalType(scale, precision)).named(columnName);
        } else if (precision < MAX_INT_PRECISION) {
            return primitive(PrimitiveTypeName.INT32, nullable).named(columnName);
        } else {
            return primitive(PrimitiveTypeName.INT64, nullable).named(columnName);
        }
    }

    private static Types.PrimitiveBuilder<PrimitiveType> primitive(PrimitiveTypeName typeName,
            int nullable) {
        return nullable == ResultSetMetaData.columnNoNulls
                ? Types.required(typeName)
                : Types.optional(typeName);
    }
}
