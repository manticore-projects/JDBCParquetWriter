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
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.util.TablesNamesFinder;

import java.util.HashSet;
import java.util.Set;

public class TableNamesFinder extends TablesNamesFinder<Void> {
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

    @Override
    public <S> Void visit(Insert insert, S context) {
        targetTableName = insert.getTable().getFullyQualifiedName();

        if (insert.getColumns() != null) {
            insert.getColumns().accept(this);
        }
        if (insert.getSelect() != null) {
            visit(insert.getSelect());
        }
        return null;
    }

    @Override
    public <S> Void visit(Merge merge, S context) {
        targetTableName = merge.getTable().getFullyQualifiedName();

        if (merge.getWithItemsList() != null) {
            for (WithItem withItem : merge.getWithItemsList()) {
                withItem.accept((SelectVisitor) this, context);
            }
        }
        merge.getTable().accept(this, context);
        return null;
    }

    @Override
    public <S> Void visit(Update update, S context) {
        targetTableName = update.getTable().getFullyQualifiedName();

        if (update.getWithItemsList() != null) {
            for (WithItem withItem : update.getWithItemsList()) {
                withItem.accept((SelectVisitor) this, context);
            }
        }

        if (update.getStartJoins() != null) {
            for (Join join : update.getStartJoins()) {
                join.getRightItem().accept(this, context);
            }
        }
        if (update.getExpressions() != null) {
            for (Expression expression : update.getExpressions()) {
                expression.accept(this, context);
            }
        }

        if (update.getFromItem() != null) {
            update.getFromItem().accept(this, context);
        }

        if (update.getJoins() != null) {
            for (Join join : update.getJoins()) {
                join.getRightItem().accept(this, context);
            }
        }

        if (update.getWhere() != null) {
            update.getWhere().accept(this, context);
        }
        return null;
    }


    @Override
    public <S> Void visit(Delete delete, S context) {
        targetTableName = delete.getTable().getFullyQualifiedName();

        if (delete.getWithItemsList() != null) {
            for (WithItem withItem : delete.getWithItemsList()) {
                withItem.accept((SelectVisitor) this, context);
            }
        }

        if (delete.getUsingList() != null) {
            for (Table using : delete.getUsingList()) {
                visit(using, context);
            }
        }

        if (delete.getJoins() != null) {
            for (Join join : delete.getJoins()) {
                join.getRightItem().accept(this, context);
            }
        }

        if (delete.getWhere() != null) {
            delete.getWhere().accept(this, context);
        }

        return null;
    }

}
