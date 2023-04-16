/**
 * Copyright (C) 2023 Andreas Reichel <andreas@manticore-projects.com>
 *
 *  This program is free software; you can redistribute it and/or modify it under the terms of the
 *  GNU Lesser General Public License as published by the Free Software Foundation; either version 3
 *  of the License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 *  even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License along with this program;
 *  if not, write to the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 *  02110-1301, USA.
 */
package com.manticore.jdbc.parquetwriter;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
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
                        + ")\n"
                        + ";",
                "INSERT INTO TEST.EXECUTION_REF VALUES (1, {d '2021-01-06'}, {ts '2021-01-15 07:48:40.851'});",
                "INSERT INTO TEST.EXECUTION_REF VALUES (2, {d '2021-01-12'}, {ts '2021-01-13 06:55:10.329'});",
                "INSERT INTO TEST.EXECUTION_REF VALUES (3, {d '2021-01-13'}, {ts '2021-01-14 05:00:41.136'});",
                "INSERT INTO TEST.EXECUTION_REF VALUES (4, {d '2021-01-14'}, {ts '2021-01-15 01:03:56.375'});",
                "INSERT INTO TEST.EXECUTION_REF VALUES (5, {d '2021-01-15'}, {ts '2021-01-16 00:19:20.212'});"
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

        String sqlStr = "SELECT * FROM test.execution_ref";
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sqlStr);) {
            JDBCParquetWriter.write(file, tableName, rs);
        }

        Assertions.assertTrue(file.exists());
        Assertions.assertTrue(file.canRead());
        Assertions.assertTrue(file.length() > 0);
    }

    @Test
    @Disabled
    void getParquetSchemaFromResultSet() {}
}
