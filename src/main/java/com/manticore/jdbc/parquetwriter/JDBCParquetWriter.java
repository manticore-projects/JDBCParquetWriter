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
import org.apache.parquet.schema.Types;

import java.io.File;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.logging.Logger;

public class JDBCParquetWriter {
    public final static Logger LOGGER = Logger.getLogger(JDBCParquetWriter.class.getName());

    public enum Dialect {
        DUCKDB, CLICKHOUSE
    };

    public static String writeFileForQueryResult(File folder, String qryStr, String targetTableName,
            Connection conn, Dialect dialect, CompressionCodecName compressionCodecName)
            throws Exception {
        File file = new File(folder, targetTableName + ".parquet");
        try (
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(qryStr);) {
            write(file, targetTableName, rs, compressionCodecName);
            LOGGER.info("Wrote parquet file: " + file.getAbsolutePath());
        }

        String importQryStr = "SET memory_limit='2GB'; INSERT INTO " + targetTableName
                + " SELECT * FROM read_parquet(\"" + file.getAbsolutePath() + "\");";
        LOGGER.info("DuckDB Insert: " + importQryStr);

        return importQryStr;
    }

    public static String writeFilesForQueryTables(File folder, String qryStr, Connection conn,
            Dialect dialect, CompressionCodecName compressionCodecName) throws Exception {
        String importQryStr = "";

        TableNamesFinder finder = new TableNamesFinder(qryStr);
        for (String tableName : finder.getSourceTableNames()) {
            LOGGER.info("Create parquet file for table: " + tableName);

            importQryStr += "\n"
                    + writeFileForQueryResult(
                            folder, "SELECT * FROM " + tableName, tableName, conn, dialect,
                            compressionCodecName);
        }
        return importQryStr;
    }

    public static void write(File file, String tableName, Connection conn) throws Exception {
        write(file, tableName, conn, CompressionCodecName.SNAPPY);
    }

    public static void write(File file, String tableName, Connection conn,
            CompressionCodecName compressionCodecName) throws Exception {
        String qryStr = "SELECT * FROM " + tableName;
        try (
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(qryStr);) {
            write(file, tableName, rs, compressionCodecName);
            LOGGER.info("Wrote parquet file: " + file.getAbsolutePath());
        }
    }

    public static void write(File f, String tableName, ResultSet rs) throws Exception {
        write(f, tableName, rs, CompressionCodecName.SNAPPY);
    }

    public static void write(File f, String tableName, ResultSet rs,
            CompressionCodecName compressionCodecName) throws Exception {
        // needed for org.apache.hadoop.util.Shell
        System.setProperty("hadoop.home.dir", f.getParent());

        Path outputPath = new Path(f.toURI());

        ResultSetMetaData metaData = rs.getMetaData();
        // SELECT FROM tablename is valid, but does not return any columns
        if (metaData.getColumnCount() == 0) {
            throw new Exception("ResultSet without any Columns. Please verify your Query.");
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
            int columnCount = metaData.getColumnCount();
            while (rs.next()) {
                Group group = new SimpleGroup(schema);
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    int columnType = metaData.getColumnType(i);
                    int scale = metaData.getScale(i);
                    int precision = metaData.getPrecision(i);

                    switch (columnType) {
                        case java.sql.Types.BOOLEAN:
                            boolean booleanValue = rs.getBoolean(i);
                            if (!rs.wasNull()) {
                                group.add(columnName, booleanValue);
                            }
                            break;
                        case java.sql.Types.BIGINT:
                        case java.sql.Types.INTEGER:
                        case java.sql.Types.SMALLINT:
                        case java.sql.Types.TINYINT:
                            int intValue = rs.getInt(i);
                            if (!rs.wasNull()) {
                                group.add(columnName, intValue);
                            }
                            break;
                        case java.sql.Types.FLOAT:
                        case java.sql.Types.REAL:
                            float floatValue = rs.getFloat(i);
                            if (!rs.wasNull()) {
                                group.add(columnName, floatValue);
                            }
                            break;
                        case java.sql.Types.DOUBLE:
                            double doubleValue = rs.getDouble(i);
                            if (!rs.wasNull()) {
                                group.add(columnName, doubleValue);
                            }
                            break;
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
                        case java.sql.Types.DATE:
                            Date d = rs.getDate(i);
                            if (!rs.wasNull()) {
                                group.add(columnName, (int) d.toLocalDate().toEpochDay());
                            }
                            break;
                        case java.sql.Types.TIME:
                            Time t = rs.getTime(i);
                            if (!rs.wasNull()) {
                                group.add(columnName, t.getTime());
                            }
                            break;
                        case java.sql.Types.TIMESTAMP:
                            Timestamp ts = rs.getTimestamp(i);
                            if (!rs.wasNull()) {
                                group.add(columnName, ts.getTime());
                            }
                            break;
                        case java.sql.Types.DECIMAL:
                        case java.sql.Types.NUMERIC:
                            BigDecimal decimal = rs.getBigDecimal(i);
                            if (!rs.wasNull()) {
                                if (scale > 0 && precision <= 18) {
                                    group.add(columnName, decimal.unscaledValue().longValue());
                                } else if (scale > 0 && precision > 18) {
                                    byte[] bytes = decimal.unscaledValue().toByteArray();
                                    group.add(columnName, Binary.fromConstantByteArray(bytes));
                                } else if (precision < 5) {
                                    group.add(columnName, decimal.intValue());
                                } else {
                                    group.add(columnName, decimal.longValue());
                                }
                            }
                            break;
                        default:
                            throw new IllegalArgumentException(
                                    "Unsupported SQL type: " + columnType);
                    }
                }
                writer.write(group);
            }
        }
    }

    public static MessageType getParquetSchemaFromResultSet(String tableName,
            ResultSetMetaData metadata) throws SQLException {
        int columnCount = metadata.getColumnCount();

        Types.MessageTypeBuilder builder = Types.buildMessage();
        for (int i = 1; i <= columnCount; i++) {
            String columnName = metadata.getColumnName(i);
            int columnType = metadata.getColumnType(i);
            int precision = metadata.getPrecision(i);
            int scale = metadata.getScale(i);
            int nullable = metadata.isNullable(i);

            switch (columnType) {
                case java.sql.Types.BOOLEAN:
                    builder.addField((nullable == ResultSetMetaData.columnNoNulls
                            ? Types.required(PrimitiveType.PrimitiveTypeName.BOOLEAN)
                            : Types.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN))
                            .named(columnName));
                    break;
                case java.sql.Types.BIGINT:
                case java.sql.Types.INTEGER:
                case java.sql.Types.SMALLINT:
                case java.sql.Types.TINYINT:
                    builder.addField((nullable == ResultSetMetaData.columnNoNulls
                            ? Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                            : Types.optional(PrimitiveType.PrimitiveTypeName.INT32))
                            .named(columnName));
                    break;
                case java.sql.Types.FLOAT:
                case java.sql.Types.REAL:
                    builder.addField((nullable == ResultSetMetaData.columnNoNulls
                            ? Types.required(PrimitiveType.PrimitiveTypeName.FLOAT)
                            : Types.optional(PrimitiveType.PrimitiveTypeName.FLOAT))
                            .named(columnName));
                    break;
                case java.sql.Types.DOUBLE:
                    builder.addField((nullable == ResultSetMetaData.columnNoNulls
                            ? Types.required(PrimitiveType.PrimitiveTypeName.DOUBLE)
                            : Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE))
                            .named(columnName));
                    break;
                case java.sql.Types.VARCHAR:
                case java.sql.Types.NVARCHAR:
                case java.sql.Types.LONGVARCHAR:
                case java.sql.Types.LONGNVARCHAR:
                case java.sql.Types.CHAR:
                case java.sql.Types.NCHAR:
                    builder.addField((nullable == ResultSetMetaData.columnNoNulls
                            ? Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                    .as(LogicalTypeAnnotation.stringType())
                            : Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                    .as(LogicalTypeAnnotation.stringType()))
                            .named(columnName));
                    break;
                case java.sql.Types.BLOB:
                case java.sql.Types.BINARY:
                case java.sql.Types.CLOB:
                case java.sql.Types.NCLOB:
                    builder.addField((nullable == ResultSetMetaData.columnNoNulls
                            ? Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                            : Types.optional(PrimitiveType.PrimitiveTypeName.BINARY))
                            .named(columnName));
                    break;
                case java.sql.Types.DATE:
                    builder.addField((nullable == ResultSetMetaData.columnNoNulls
                            ? Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                                    .as(LogicalTypeAnnotation.dateType())
                            : Types.optional(PrimitiveType.PrimitiveTypeName.INT32)
                                    .as(LogicalTypeAnnotation.dateType()))
                            .named(columnName));
                    break;
                case java.sql.Types.TIME:
                    builder.addField((nullable == ResultSetMetaData.columnNoNulls
                            ? Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                                    .as(LogicalTypeAnnotation.timeType(true,
                                            LogicalTypeAnnotation.TimeUnit.MILLIS))
                            : Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                                    .as(LogicalTypeAnnotation.timeType(true,
                                            LogicalTypeAnnotation.TimeUnit.MILLIS)))
                            .named(columnName));
                    break;
                case java.sql.Types.TIMESTAMP:
                    builder.addField((nullable == ResultSetMetaData.columnNoNulls
                            ? Types.required(PrimitiveType.PrimitiveTypeName.INT64).as(
                                    LogicalTypeAnnotation.timestampType(true,
                                            LogicalTypeAnnotation.TimeUnit.MILLIS))
                            : Types.optional(PrimitiveType.PrimitiveTypeName.INT64).as(
                                    LogicalTypeAnnotation.timestampType(true,
                                            LogicalTypeAnnotation.TimeUnit.MILLIS)))
                            .named(columnName));
                    break;
                case java.sql.Types.DECIMAL:
                case java.sql.Types.NUMERIC:
                    // PRECISION <= 18 can be transported as LONG derived from an unscaled
                    // BigInteger
                    // PRECISION > 18 must be transported as Binary based on the unscaled
                    // BigInteger's bytes
                    if (scale > 0 && precision <= 18) {
                        builder.addField((nullable == ResultSetMetaData.columnNoNulls
                                ? Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                                        .as(LogicalTypeAnnotation.decimalType(scale, precision))
                                : Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                                        .as(LogicalTypeAnnotation.decimalType(scale, precision)))
                                .named(columnName));
                    } else if (scale > 0 && precision > 18) {
                        builder.addField((nullable == ResultSetMetaData.columnNoNulls
                                ? Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                                        .as(LogicalTypeAnnotation.decimalType(scale, precision))
                                : Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                                        .as(LogicalTypeAnnotation.decimalType(scale, precision)))
                                .named(columnName));
                    } else if (precision < 5) {
                        builder.addField((nullable == ResultSetMetaData.columnNoNulls
                                ? Types.required(PrimitiveType.PrimitiveTypeName.INT32)
                                : Types.optional(PrimitiveType.PrimitiveTypeName.INT32))
                                .named(columnName));
                    } else {
                        builder.addField((nullable == ResultSetMetaData.columnNoNulls
                                ? Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                                : Types.optional(PrimitiveType.PrimitiveTypeName.INT64))
                                .named(columnName));
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported SQL type: " + columnType);
            }
        }
        return builder.named(tableName);
    }
}

