package com.manticore.jdbc.parquetwriter;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statements;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;

import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Random;
import java.util.logging.Logger;

@State(Scope.Benchmark)
@Threads(1)
public class PerformanceTest {
    private final static Logger LOGGER = Logger.getLogger(PerformanceTest.class.getName());
    static Connection connH2;
    static Connection connDuck;

    private final static int SAMPLE_SIZE = 1000000;

    private final static String[] KEY_COLUMNS = {"PRODUCT", "SEGMENT", "TYPE", "IMPAIRMENT_STAGE"};

    @BeforeAll
    @Setup(Level.Trial)
    public static void init() throws Exception {
        connH2 = DriverManager.getConnection("jdbc:h2:~/test");
        connDuck = DriverManager.getConnection("jdbc:duckdb:");

        // Create the Table with Indices
        String sqlStr = IOUtils.resourceToString("test_ddl.sql", Charset.defaultCharset(),
                PerformanceTest.class.getClassLoader());
        Statements statements = CCJSqlParserUtil.parseStatements(sqlStr);
        try (Statement st = connH2.createStatement()) {
            for (net.sf.jsqlparser.statement.Statement statement : statements.getStatements()) {
                LOGGER.info("execute: " + statement.toString());
                st.execute(statement.toString());
            }
        }

        LOGGER.info("Start writing sample portfolio");
        // Populate the table with samples
        sqlStr = IOUtils.resourceToString("test_insert.sql", Charset.defaultCharset(),
                PerformanceTest.class.getClassLoader());
        try (PreparedStatement st = connH2.prepareStatement(sqlStr)) {
            int len = String.valueOf(SAMPLE_SIZE).length();
            Random random = new Random();
            Object[][] keys = {
                    {0.125 * 0.83, new String[] {"a", "regular", "s", "1"}},
                    {0.125 * 0.15, new String[] {"a", "regular", "s", "2"}},
                    {0.125 * 0.02, new String[] {"a", "regular", "s", "3"}},
                    {0.125 * 0.83, new String[] {"a", "regular", "k", "1"}},
                    {0.125 * 0.15, new String[] {"a", "regular", "k", "2"}},
                    {0.125 * 0.02, new String[] {"a", "regular", "k", "3"}},
                    {0.125 * 0.83, new String[] {"a", "plus", "s", "1"}},
                    {0.125 * 0.15, new String[] {"a", "plus", "s", "2"}},
                    {0.125 * 0.02, new String[] {"a", "plus", "s", "3"}},
                    {0.125 * 0.83, new String[] {"a", "plus", "k", "1"}},
                    {0.125 * 0.15, new String[] {"a", "plus", "k", "2"}},
                    {0.125 * 0.02, new String[] {"a", "plus", "k", "3"}},
                    {0.125 * 0.83, new String[] {"b", "regular", "s", "1"}},
                    {0.125 * 0.15, new String[] {"b", "regular", "s", "2"}},
                    {0.125 * 0.02, new String[] {"b", "regular", "s", "3"}},
                    {0.125 * 0.83, new String[] {"b", "regular", "k", "1"}},
                    {0.125 * 0.15, new String[] {"b", "regular", "k", "2"}},
                    {0.125 * 0.02, new String[] {"b", "regular", "k", "3"}},
                    {0.125 * 0.83, new String[] {"b", "plus", "s", "1"}},
                    {0.125 * 0.15, new String[] {"b", "plus", "s", "2"}},
                    {0.125 * 0.02, new String[] {"b", "plus", "s", "3"}},
                    {0.125 * 0.83, new String[] {"b", "plus", "k", "1"}},
                    {0.125 * 0.15, new String[] {"b", "plus", "k", "2"}},
                    {0.125 * 0.02, new String[] {"b", "plus", "k", "3"}}
            };
            for (int i = 1; i < SAMPLE_SIZE; i++) {
                double rand = random.nextDouble();
                BigDecimal salt = BigDecimal
                        .valueOf(8.5d + 3 * rand )
                        .scaleByPowerOfTen(-1);

                double totalWeight = 0.0;
                for (int k = 0; k < keys.length; k++) {
                    double weight = (Double) keys[k][0];
                    totalWeight += weight;
                    if (rand < totalWeight || k == keys.length - 1) {
                        String idInstrument = StringUtils.leftPad(String.valueOf(i), len, "0");
                        String[] attributes = (String[]) keys[k][1];
                        BigDecimal remainingPrincipal = BigDecimal.valueOf(-1000000).multiply(salt)
                                .setScale(2, RoundingMode.HALF_EVEN);
                        BigDecimal amortisedCost = BigDecimal.valueOf(-1100000).multiply(salt)
                                .setScale(2, RoundingMode.HALF_EVEN);
                        BigDecimal eir = BigDecimal.valueOf(0.25737372).multiply(salt).setScale(9,
                                RoundingMode.HALF_EVEN);
                        BigDecimal interestIncome = BigDecimal.valueOf(57000).multiply(salt)
                                .setScale(2, RoundingMode.HALF_EVEN);
                        BigDecimal unamortisedFee = rand > 0.66
                                ? BigDecimal.valueOf(90000).multiply(salt).setScale(2,
                                        RoundingMode.HALF_EVEN)
                                : null;
                        BigDecimal amortisedFeePL = rand > 0.66
                                ? unamortisedFee.multiply(BigDecimal.valueOf(0.15)).negate()
                                        .setScale(2, RoundingMode.HALF_EVEN)
                                : null;
                        int impairmentStage = Integer.parseInt(attributes[3]);

                        BigDecimal incomeAdjustment = impairmentStage == 3
                                ? interestIncome.multiply(BigDecimal.valueOf(0.25)).negate()
                                        .setScale(2, RoundingMode.HALF_EVEN)
                                : BigDecimal.ZERO;
                        st.clearParameters();
                        st.setString(1, idInstrument);
                        st.setString(2, attributes[0]);
                        st.setString(3, attributes[1]);
                        st.setString(4, attributes[2]);
                        st.setString(5, "USD");
                        st.setBigDecimal(6, remainingPrincipal);
                        st.setBigDecimal(7, amortisedCost);
                        st.setBigDecimal(8, eir);
                        st.setBigDecimal(9, interestIncome);
                        st.setBigDecimal(10, unamortisedFee);
                        st.setBigDecimal(11, amortisedFeePL);
                        st.setInt(12, impairmentStage);
                        st.setBigDecimal(13, interestIncome);
                        st.setBigDecimal(14, interestIncome);
                        st.setBigDecimal(15, interestIncome);
                        st.setBigDecimal(16, interestIncome);
                        st.setBigDecimal(17, incomeAdjustment);
                        st.addBatch();

                        break;
                    }
                }

                if (i % 10000 == 0) {
                    st.executeBatch();
                    LOGGER.info(i + " records written;");
                }
            }
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
    @TearDown
    public static void close() throws SQLException {
        connH2.close();
        connDuck.close();
    }

    private Object[] queryDB(Connection conn) {
        ArrayList<Object> results = new ArrayList<>();

        for (String key : KEY_COLUMNS) {
            String sqlString = "SELECT Count(*) FROM (SELECT DISTINCT " + key + " FROM test)";
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery(sqlString)) {
                if (rs.next()) {
                    results.add(rs.getInt(1));
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        String sqlString =
                "SELECT product, segment, type, impairment_stage, Count(*), Sum(amortised_cost) FROM test group by product, segment, type, impairment_stage";
        try (Statement st = connH2.createStatement();
             ResultSet rs = st.executeQuery(sqlString)) {
            if (rs.next()) {
                results.add(rs.getInt(5));
                results.add(rs.getBigDecimal(6));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return results.toArray();
    }

    @Test
    @Benchmark
    public void queryH2() {
        Object[] results = queryDB(connH2);
        LOGGER.info("returned records:" + results.length);
    }

    @Test
    @Benchmark
    public void testDuckDB() {
        Object[] results = queryDB(connDuck);
        LOGGER.info("returned records:" + results.length);
    }
}
