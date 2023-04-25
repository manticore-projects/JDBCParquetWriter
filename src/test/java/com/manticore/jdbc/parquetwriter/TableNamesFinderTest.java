package com.manticore.jdbc.parquetwriter;

import net.sf.jsqlparser.JSQLParserException;
import org.apache.commons.collections.CollectionUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

class TableNamesFinderTest {

    @Test
    void testSimpleSelectJoin() throws JSQLParserException {
        String sqlStr = "SELECT * from a inner join b using (c) left join d using (e)";
        TableNamesFinder finder = new TableNamesFinder(sqlStr);

        Assertions.assertTrue(
                CollectionUtils.isEqualCollection(
                        Arrays.asList("a", "b", "d"), finder.getSourceTableNames()));

        Assertions.assertNull(finder.getTargetTableName());
    }

    @Test
    void testComplexInsert() throws JSQLParserException {
        String sqlStr =
                "\n"
                        + "INSERT /*+ PARALLEL(4) APPEND DYNAMIC_SAMPLING(0) */ INTO cfe.ext_ledger_branch_bal_details\n"
                        + "WITH scope AS (\n"
                        + "        SELECT *\n"
                        + "        FROM cfe.accounting_scope\n"
                        + "        WHERE id_status = 'C'\n"
                        + "            AND id_accounting_scope_code = 'IFRS_9' )\n"
                        + "    , ex AS (\n"
                        + "        SELECT *\n"
                        + "        FROM cfe.execution\n"
                        + "        WHERE id_status = 'R'\n"
                        + "            AND value_date = (  SELECT Max( value_date )\n"
                        + "                                FROM cfe.execution\n"
                        + "                                WHERE id_status = 'R'\n"
                        + "                                    AND ( to_Date( :VALUE_DATE, 'MM/DD/YY') IS NULL\n"
                        + "                                            OR value_date <= to_Date( :VALUE_DATE, 'MM/DD/YY') ) ) )\n"
                        + "    , fxr AS (\n"
                        + "        SELECT  id_currency_from\n"
                        + "                , fxrate\n"
                        + "        FROM common.fxrate_hst f\n"
                        + "            INNER JOIN ex\n"
                        + "                ON f.value_date <= ex.value_date\n"
                        + "        WHERE f.value_date = (  SELECT Max( value_date )\n"
                        + "                                FROM common.fxrate_hst\n"
                        + "                                WHERE id_currency_from = f.id_currency_from\n"
                        + "                                    AND id_currency_into = f.id_currency_into\n"
                        + "                                    AND value_date <= ex.value_date )\n"
                        + "            AND id_currency_into = 'NGN'\n"
                        + "        UNION ALL\n"
                        + "        SELECT  'NGN'\n"
                        + "                , 1\n"
                        + "        FROM dual )\n"
                        + "SELECT /*+ PARALLEL(4) DYNAMIC_SAMPLING(0) */\n"
                        + "    scope.id_accounting_scope\n"
                        + "    , ex.value_date\n"
                        + "    , ex.posting_date\n"
                        + "    , a.gl_level\n"
                        + "    , a.code\n"
                        + "    , c.id_instrument_ref\n"
                        + "    , c.id_currency\n"
                        + "    , CASE\n"
                        + "            WHEN s.book_value <= 0\n"
                        + "                THEN c.balance_debit + c.balance_credit\n"
                        + "            ELSE 0\n"
                        + "        END balance_debit\n"
                        + "    , CASE\n"
                        + "            WHEN s.book_value > 0\n"
                        + "                THEN c.balance_debit + c.balance_credit\n"
                        + "            ELSE 0\n"
                        + "        END balance_credit\n"
                        + "    , CASE\n"
                        + "            WHEN s.book_value <= 0\n"
                        + "                THEN c.balance_debit_bc + c.balance_credit_bc\n"
                        + "            ELSE 0\n"
                        + "        END balance_debit_bc\n"
                        + "    , CASE\n"
                        + "            WHEN s.book_value > 0\n"
                        + "                THEN c.balance_debit_bc + c.balance_credit_bc\n"
                        + "            ELSE 0\n"
                        + "        END balance_credit_bc\n"
                        + "FROM ex\n"
                        + "    , scope\n"
                        + "    INNER JOIN cfe.ledger_branch_branch a\n"
                        + "        ON a.id_accounting_scope = scope.id_accounting_scope\n"
                        + "            AND a.code = a.code_inferior\n"
                        + "    INNER JOIN cfe.ledger_branch b\n"
                        + "        ON b.id_accounting_scope = scope.id_accounting_scope\n"
                        + "            AND b.code = a.code\n"
                        + "    INNER JOIN (    SELECT  b.code\n"
                        + "                            , id_instrument_ref\n"
                        + "                            , id_currency\n"
                        + "                            , Sum( d.balance_debit ) balance_debit\n"
                        + "                            , Sum( d.balance_credit ) balance_credit\n"
                        + "                            , Round( Sum( d.balance_debit * fxr.fxrate ), 2 ) balance_debit_bc\n"
                        + "                            , Round( Sum( d.balance_credit * fxr.fxrate ), 2 ) balance_credit_bc\n"
                        + "                    FROM scope\n"
                        + "                        INNER JOIN cfe.ledger_branch_branch b\n"
                        + "                            ON b.id_accounting_scope = scope.id_accounting_scope\n"
                        + "                        INNER JOIN cfe.ledger_account c\n"
                        + "                            ON b.code_inferior = c.code\n"
                        + "                                AND c.id_accounting_scope_code = scope.id_accounting_scope_code\n"
                        + "                        INNER JOIN (    SELECT  id_account\n"
                        + "                                                , id_instrument_ref\n"
                        + "                                                , Sum( amount_debit ) balance_debit\n"
                        + "                                                , Sum( amount_credit ) balance_credit\n"
                        + "                                        FROM (  SELECT  id_account_credit id_account\n"
                        + "                                                        , id_instrument_ref\n"
                        + "                                                        , 0 amount_debit\n"
                        + "                                                        , amount amount_credit\n"
                        + "                                                FROM cfe.ledger_account_entry\n"
                        + "                                                    INNER JOIN ex\n"
                        + "                                                        ON ledger_account_entry.posting_date <= ex.posting_date\n"
                        + "                                                    INNER JOIN cfe.ledger_account c\n"
                        + "                                                        ON ledger_account_entry.id_account_credit = c.id_account\n"
                        + "                                                            AND c.id_accounting_scope_code = 'IFRS_9'\n"
                        + "                                                UNION ALL\n"
                        + "                                                SELECT  id_account_debit\n"
                        + "                                                        , id_instrument_ref\n"
                        + "                                                        , - amount\n"
                        + "                                                        , 0\n"
                        + "                                                FROM cfe.ledger_account_entry\n"
                        + "                                                    INNER JOIN ex\n"
                        + "                                                        ON ledger_account_entry.posting_date <= ex.posting_date\n"
                        + "                                                    INNER JOIN cfe.ledger_account c\n"
                        + "                                                        ON ledger_account_entry.id_account_debit = c.id_account\n"
                        + "                                                            AND c.id_accounting_scope_code = 'IFRS_9'\n"
                        + "                                                UNION ALL\n"
                        + "                                                SELECT  id_account_credit id_account\n"
                        + "                                                        , id_instrument_ref\n"
                        + "                                                        , 0\n"
                        + "                                                        , amount\n"
                        + "                                                FROM cfe.ledger_acc_entry_manual\n"
                        + "                                                    INNER JOIN ex\n"
                        + "                                                        ON ledger_acc_entry_manual.value_date <= ex.value_date\n"
                        + "                                                    INNER JOIN cfe.ledger_account c\n"
                        + "                                                        ON ledger_acc_entry_manual.id_account_credit = c.id_account\n"
                        + "                                                            AND c.id_accounting_scope_code = 'IFRS_9'\n"
                        + "                                                UNION ALL\n"
                        + "                                                SELECT  id_account_debit\n"
                        + "                                                        , id_instrument_ref\n"
                        + "                                                        , - amount\n"
                        + "                                                        , 0\n"
                        + "                                                FROM cfe.ledger_acc_entry_manual\n"
                        + "                                                    INNER JOIN ex\n"
                        + "                                                        ON ledger_acc_entry_manual.value_date <= ex.value_date\n"
                        + "                                                    INNER JOIN cfe.ledger_account c\n"
                        + "                                                        ON ledger_acc_entry_manual.id_account_debit = c.id_account\n"
                        + "                                                            AND c.id_accounting_scope_code = 'IFRS_9' )\n"
                        + "                                        GROUP BY    id_account\n"
                        + "                                                    , id_instrument_ref ) d\n"
                        + "                            ON c.id_account = d.id_account\n"
                        + "                        INNER JOIN fxr\n"
                        + "                            ON c.id_currency = fxr.id_currency_from\n"
                        + "                    GROUP BY    b.code\n"
                        + "                                , id_instrument_ref\n"
                        + "                                , id_currency ) c\n"
                        + "        ON c.code = a.code\n"
                        + "    LEFT JOIN ( cfe.instrument_measure_summary s\n"
                        + "                    INNER JOIN ex x\n"
                        + "                        ON s.value_date = x.value_date\n"
                        + "                            AND s.posting_date = x.posting_date )\n"
                        + "        ON s.id_instrument_ref = c.id_instrument_ref\n"
                        + ";";

        TableNamesFinder finder = new TableNamesFinder(sqlStr);

        Assertions.assertTrue(
                CollectionUtils.isEqualCollection(
                        Arrays.asList(
                                "cfe.accounting_scope", "cfe.execution", "common.fxrate_hst",
                                "dual", "cfe.ledger_branch_branch", "cfe.ledger_branch",
                                "cfe.ledger_account", "cfe.ledger_account_entry",
                                "cfe.ledger_acc_entry_manual", "cfe.instrument_measure_summary"),
                        finder.getSourceTableNames()));

        Assertions.assertEquals("cfe.ext_ledger_branch_bal_details", finder.getTargetTableName());
    }
}
