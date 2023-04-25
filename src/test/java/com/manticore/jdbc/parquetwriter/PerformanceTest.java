package com.manticore.jdbc.parquetwriter;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statements;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

public class PerformanceTest {
    private final static Logger LOGGER = Logger.getLogger(PerformanceTest.class.getName());
    static Connection connH2;
    static Connection connDuck;

    private final static String[] KEY_COLUMNS = {"PRODUCT", "SEGMENT", "TYPE", "IMPAIRMENT_STAGE"};

    @BeforeAll
    static void init() throws Exception {
        connH2 = DriverManager.getConnection("jdbc:h2:~/test");
        connDuck = DriverManager.getConnection("jdbc:duckdb:/home/are/duckdb_test");

        final URL url =
                IOUtils.resourceToURL("test.csv.bz2", PerformanceTest.class.getClassLoader());

        File file = File.createTempFile("test_", ".csv");
        file.deleteOnExit();

        try (
                InputStream inputStream = url.openStream();
                BZip2CompressorInputStream inputStream1 =
                        new BZip2CompressorInputStream(inputStream);
                FileOutputStream fileOutputStream = new FileOutputStream(file);) {

            IOUtils.copyLarge(inputStream1, fileOutputStream);
        }

        String sqlStr = IOUtils.resourceToString("test_ddl.sql", Charset.defaultCharset(),
                PerformanceTest.class.getClassLoader());
        Statements statements = CCJSqlParserUtil.parseStatements(sqlStr);

        try (Statement st = connH2.createStatement()) {
            for (net.sf.jsqlparser.statement.Statement statement : statements.getStatements()) {
                LOGGER.info("execute: " + statement.toString());
                st.execute(statement.toString());
            }
            st.execute("INSERT INTO test SELECT * FROM CsvRead('" + file.getAbsolutePath() + "')");
        }

        File parquetFile = File.createTempFile("test_", ".parquet");
        parquetFile.deleteOnExit();
        JDBCParquetWriter.write(parquetFile, "TEST", connH2);

        try (Statement st = connDuck.createStatement()) {
            for (net.sf.jsqlparser.statement.Statement statement : statements.getStatements()) {
                LOGGER.info("execute: " + statement.toString());
                st.execute(statement.toString());
            }
            st.execute("INSERT INTO test SELECT * FROM '" + parquetFile.getAbsolutePath() + "'");
        }
        LOGGER.info("Finished preparation");
    }

    @AfterAll
    static void close() throws SQLException {
        connH2.close();
        connDuck.close();
    }

    @Test
    void queryH2() throws SQLException {
        for (String key : KEY_COLUMNS) {
            String sqlString = "SELECT Count(*) FROM (SELECT DISTINCT " + key + " FROM test)";
            try (Statement st = connH2.createStatement();
                    ResultSet rs = st.executeQuery(sqlString)) {
                if (rs.next()) {
                    LOGGER.info(key + ":\t" + rs.getInt(1) + " distinct values.");
                }
            }
        }
    }

    @Test
    void testDuckDB() throws Exception {
        for (String key : KEY_COLUMNS) {
            String sqlString = "SELECT Count(*) FROM (SELECT DISTINCT " + key + " FROM test)";
            try (Statement st = connDuck.createStatement();
                    ResultSet rs = st.executeQuery(sqlString)) {
                if (rs.next()) {
                    LOGGER.info(key + ":\t" + rs.getInt(1) + " distinct values.");
                }
            }
        }
    }

    @Test
    void testCSV() throws IOException, SQLException {
        String data = "\"Test\"\n\"2010.00\"\n\"\"";
        File file = File.createTempFile("test_", ".csv");
        file.deleteOnExit();

        try (FileOutputStream outputStream = new FileOutputStream(file);) {
            IOUtils.write(data, outputStream);
        }

        Connection conn = DriverManager.getConnection("jdbc:h2:mem:test");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE TEST(TEST DECIMAL(12,2) NULL)");
        stat.execute("INSERT INTO TEST SELECT * FROM CsvRead('" + file.getAbsolutePath() + "')");
    }
}
