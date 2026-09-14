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
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.merge.Merge;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.util.TablesNamesFinder;

import java.util.HashSet;
import java.util.Set;

/**
 * Splits the tables of a statement into the one DML target table and the source tables feeding it.
 * <p>
 * The traversal itself is left entirely to {@link TablesNamesFinder}: the overrides below only
 * record the target table on the way through and then delegate. Re-implementing the DML visitors
 * here means missing every clause JSqlParser adds later (RETURNING, FOR PORTION OF, Oracle
 * multi-table INSERT, ...) and tracking a moving API; delegating keeps this class at a dozen lines.
 */
public class TableNamesFinder extends TablesNamesFinder<Void> {
    private final Set<String> sourceTables = new HashSet<>();
    private String targetTableName;

    public TableNamesFinder(String sqlStr) throws JSQLParserException {
        Statement statement = CCJSqlParserUtil.parse(sqlStr);

        // getTables() performs init() and the traversal itself, so there is no need to call
        // either here; the overrides below run as part of it and capture the target table
        sourceTables.addAll(getTables(statement));

        // the DML target receives data, it is not a source of it
        sourceTables.remove(targetTableName);
    }

    /** The table an INSERT, UPDATE, DELETE or MERGE writes into, or {@code null} for a SELECT. */
    public String getTargetTableName() {
        return targetTableName;
    }

    /** The tables the statement reads from; the returned set is a defensive copy. */
    public Set<String> getSourceTableNames() {
        return new HashSet<>(sourceTables);
    }

    @Override
    public <S> Void visit(Insert insert, S context) {
        // null for an Oracle multi-table INSERT, which carries its targets per branch
        if (insert.getTable() != null) {
            targetTableName = insert.getTable().getFullyQualifiedName();
        }
        return super.visit(insert, context);
    }

    @Override
    public <S> Void visit(Update update, S context) {
        targetTableName = update.getTable().getFullyQualifiedName();
        return super.visit(update, context);
    }

    @Override
    public <S> Void visit(Delete delete, S context) {
        targetTableName = delete.getTable().getFullyQualifiedName();
        return super.visit(delete, context);
    }

    @Override
    public <S> Void visit(Merge merge, S context) {
        targetTableName = merge.getTable().getFullyQualifiedName();
        return super.visit(merge, context);
    }
}
