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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

class JDBCParquetWriterTest {
    static Connection conn;

    @BeforeAll
    static void init() throws SQLException {
        conn = DriverManager.getConnection("jdbc:h2:mem:test");

        String[] ddlStr = {
                "CREATE SCHEMA test;",
                "CREATE TABLE test.execution_ref (\n"
                        + "    id_execution_ref         DECIMAL(9) NOT NULL\n"
                        + "    , value_date             DATE       NOT NULL\n"
                        + "    , posting_date           TIMESTAMP  NOT NULL\n"
                        + "    , amount                 DECIMAL(12,2)  NULL\n"
                        + "    , large_amount           DECIMAL(23,5)  NULL\n"
                        + ")\n"
                        + ";",
                "INSERT INTO TEST.EXECUTION_REF "
                        + "VALUES (1, {d '2021-01-06'}, {ts '2021-01-15 07:48:40.851'}, 100.22 , 100.22);",
                "INSERT INTO TEST.EXECUTION_REF "
                        + "VALUES (2, {d '2021-01-12'}, {ts '2021-01-13 06:55:10.329'}, 75.3, 75.3);",
                "INSERT INTO TEST.EXECUTION_REF "
                        + "VALUES (3, {d '2021-01-13'}, {ts '2021-01-14 05:00:41.136'}, null, null);",
                "INSERT INTO TEST.EXECUTION_REF "
                        + "VALUES (4, {d '2021-01-14'}, {ts '2021-01-15 01:03:56.375'}, 354.1, 354.1);",
                "INSERT INTO TEST.EXECUTION_REF "
                        + "VALUES (5, {d '2021-01-15'}, {ts '2021-01-16 00:19:20.212'}, 7.15, 7.15);",
                "INSERT INTO TEST.EXECUTION_REF "
                        + " VALUES (6, {d '2024-08-30'}, {ts '2024-11-14 05:22:46.385'}, -290653956.00, 2832724.01000);\n"
        };

        try (Statement st = conn.createStatement()) {
            for (String sqlStr : ddlStr) {
                st.execute(sqlStr);
            }
        }
    }

    @AfterAll
    static void close() throws SQLException {
        conn.close();
    }

    @Test
    void write() throws Exception {
        String tableName = "execution_ref";
        File file = File.createTempFile(tableName, ".parquet");
        long writtenRows = 0;

        String sqlStr = "SELECT * FROM test.execution_ref";
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sqlStr);) {
            writtenRows = JDBCParquetWriter.write(file, tableName, rs);
        }

        Assertions.assertTrue(file.exists());
        Assertions.assertTrue(file.canRead());
        Assertions.assertTrue(file.length() > 0);
        Assertions.assertEquals(6, writtenRows);
    }

    @Test
    void testBigDecimal() throws Exception {
        String tableName = "test";
        String decimalStr = "-24999999999999.99500";

        File file = File.createTempFile(tableName, ".parquet");
        try (Connection conn = DriverManager.getConnection(
                "jdbc:h2:mem:test")) {

            String[] ddlStr = {
                    "CREATE TABLE decimal_test (\n"
                            + "   amount                 DECIMAL(23,5)  NULL\n"
                            + ")\n"
                            + ";"

                    , "INSERT INTO decimal_test \n"
                            + "VALUES (" + decimalStr + ");"
            };

            try (Statement st = conn.createStatement()) {
                for (String sqlStr : ddlStr) {
                    st.execute(sqlStr);
                }
            }

            long writtenRows = 0;
            String sqlStr = "SELECT  *\n"
                    + "FROM decimal_test";
            try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sqlStr);) {
                writtenRows = JDBCParquetWriter.write(file, tableName, rs);
            }
        }

        String sqlStr = "SELECT  *\n"
                + "FROM '" + file.getAbsolutePath() + "';";
        try (Connection conn = DriverManager.getConnection(
                "jdbc:duckdb:");
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(sqlStr)) {
            if (rs.next()) {
                BigDecimal actualDecimal = rs.getBigDecimal(1);
                Assertions.assertEquals(decimalStr, actualDecimal.toPlainString());

                final boolean delete = file.delete();
            }
        }

    }
}
