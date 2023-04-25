package com.manticore.jdbc.parquetwriter;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.merge.Merge;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.util.TablesNamesFinder;

import java.util.HashSet;
import java.util.Set;

public class TableNamesFinder extends TablesNamesFinder {
    private String targetTableName = null;
    private HashSet<String> sourceTables = new HashSet<>();
    private Statement statement;

    public TableNamesFinder(String sqlStr) throws JSQLParserException {
        statement = CCJSqlParserUtil.parse(sqlStr);

        init(false);
        statement.accept(this);

        sourceTables.addAll(getTableList(statement));
    }

    public String getTargetTableName() {
        return targetTableName;
    }

    public Set<String> getSourceTableNames() {
        return sourceTables;
    }

    public void visit(Insert insert) {
        targetTableName = insert.getTable().getFullyQualifiedName();

        if (insert.getItemsList() != null) {
            insert.getItemsList().accept(this);
        }
        if (insert.getSelect() != null) {
            visit(insert.getSelect());
        }
    }

    @Override
    public void visit(Merge merge) {
        targetTableName = merge.getTable().getFullyQualifiedName();

        if (merge.getWithItemsList() != null) {
            for (WithItem withItem : merge.getWithItemsList()) {
                withItem.accept(this);
            }
        }

        if (merge.getUsingTable() != null) {
            visit(merge.getUsingTable());
        } else if (merge.getUsingSelect() != null) {
            visit(merge.getUsingSelect());
        }
    }

    public void visit(Update update) {
        targetTableName = update.getTable().getFullyQualifiedName();

        if (update.getWithItemsList() != null) {
            for (WithItem withItem : update.getWithItemsList()) {
                withItem.accept(this);
            }
        }

        if (update.getStartJoins() != null) {
            for (Join join : update.getStartJoins()) {
                join.getRightItem().accept(this);
            }
        }
        if (update.getExpressions() != null) {
            for (Expression expression : update.getExpressions()) {
                expression.accept(this);
            }
        }

        if (update.getFromItem() != null) {
            update.getFromItem().accept(this);
        }

        if (update.getJoins() != null) {
            for (Join join : update.getJoins()) {
                join.getRightItem().accept(this);
            }
        }

        if (update.getWhere() != null) {
            update.getWhere().accept(this);
        }
    }


    @Override
    public void visit(Delete delete) {
        targetTableName = delete.getTable().getFullyQualifiedName();

        if (delete.getWithItemsList() != null) {
            for (WithItem withItem : delete.getWithItemsList()) {
                withItem.accept(this);
            }
        }

        if (delete.getUsingList() != null) {
            for (Table using : delete.getUsingList()) {
                visit(using);
            }
        }

        if (delete.getJoins() != null) {
            for (Join join : delete.getJoins()) {
                join.getRightItem().accept(this);
            }
        }

        if (delete.getWhere() != null) {
            delete.getWhere().accept(this);
        }
    }

}
