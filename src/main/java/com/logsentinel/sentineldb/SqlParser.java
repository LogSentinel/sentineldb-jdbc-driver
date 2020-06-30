package com.logsentinel.sentineldb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitorAdapter;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.FromItemVisitorAdapter;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.statement.select.SelectItemVisitorAdapter;
import net.sf.jsqlparser.statement.select.SelectVisitorAdapter;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.update.Update;

public class SqlParser {

    /**
     * cases:
     * INSERT INTO x VALUES (...);
     * INSERT INTO x (y,z) VALUES (..);
     * UPDATE x SET y=z
     * any query with WHERE x=y
     * any query with WHERE x iN (..)
     */
    
    public SqlParseResult parse(String query) {
        try {
            Statement stm = CCJSqlParserUtil.parse(query);
            if (stm instanceof Select) {
                return handleSelect(stm);
            } else if (stm instanceof Insert) {
                return handleInsert(stm);
            } else if (stm instanceof Update) {
                return handleUpdate(stm);
            } else {
                System.out.println("Unrecognized statement of type " + stm.getClass());
                return null;
            }
        } catch (JSQLParserException e) {
            e.printStackTrace();
            return null;
        }
            
    }

    public SqlParseResult handleSelect(Statement stm) {
        SqlParseResult result = new SqlParseResult();
        Select select = (Select) stm;
        select.getSelectBody().accept(new SelectClauseVisitor(result));
        return result;
        
    }

    public SqlParseResult handleUpdate(Statement stm) {
        SqlParseResult result = new SqlParseResult();
        
        Update update = (Update) stm;
        List<Column> columns = update.getColumns();
        
        List<Expression> expressions = update.getExpressions();
        List<String> values = new ArrayList<>();
        for (Expression expr : expressions) {
            if (expr instanceof StringValue) {
                values.add(((StringValue) expr).getValue()); 
            }
        }
        
        Iterator<String> valuesIterator = values.iterator();
        result.getColumns().addAll(columns.stream()
                .map(c -> new TableColumn(update.getTable().getName(), c.getColumnName(), valuesIterator.next()))
                .collect(Collectors.toList()));
        
        update.getWhere().accept(new WhereExpressionVisitor(result, Collections.emptyMap(), update.getTable().getName()));
        
        return result;
    }

    public SqlParseResult handleInsert(Statement stm) {
        SqlParseResult result = new SqlParseResult();
        
        Insert insert = (Insert) stm;
        List<Column> columns = ((Insert) stm).getColumns();
        if (columns == null || columns.isEmpty()) {
            // if no columns are specified, fetch from database (and cache)
            columns = new ArrayList<>();
        }
        
        ItemsList items = insert.getItemsList();
        List<String> values = new ArrayList<>();
        items.accept(new ItemsListVisitorAdapter() {
            @Override
            public void visit(ExpressionList expressionList) {
                List<Expression> expressions = expressionList.getExpressions();
                for (Expression expr : expressions) {
                    if (expr instanceof StringValue) {
                        values.add(((StringValue) expr).getValue()); 
                    }
                }
            }
        });
        
        Iterator<String> valuesIterator = values.iterator();
        result.getColumns().addAll(columns.stream()
                .map(c -> new TableColumn(insert.getTable().getName(), c.getColumnName(), valuesIterator.next()))
                .collect(Collectors.toList()));
        return result;
    }
    
    public static class SelectClauseVisitor extends SelectVisitorAdapter {
        private SqlParseResult result;
        public SelectClauseVisitor(SqlParseResult result) {
            this.result = result;
        }

        @Override
        public void visit(PlainSelect plainSelect) {
            Map<String, String> aliases = new HashMap<>();
            StringBuilder tableNameBuilder = new StringBuilder();
            SelectItemVisitor visitor = new SelectItemVisitorAdapter() {
                @Override
                public void visit(SelectExpressionItem item) {
                    Expression expr = item.getExpression();
                    if (expr instanceof Column) {
                        Column column = (Column) expr;
                        if (item.getAlias() != null) {
                            aliases.put(item.getAlias().getName(), column.getColumnName());
                        }
                    }
                }
            };
            
            for (Join join : plainSelect.getJoins()) {
                if (join.getRightItem().getAlias() != null) {
                    StringBuilder joinTableNameBuilder = new StringBuilder();
                    join.getRightItem().accept(new FromItemVisitorAdapter() {
                        @Override
                        public void visit(Table table) {
                            joinTableNameBuilder.append(table.getName());
                        }
                    });
                    aliases.put(join.getRightItem().getAlias().getName(), joinTableNameBuilder.toString());
                }
            }
            plainSelect.getFromItem().accept(new FromItemVisitorAdapter() {
                @Override
                public void visit(Table table) {
                    tableNameBuilder.append(table.getName());
                }
                
                @Override
                public void visit(SubSelect subSelect) {
                    subSelect.getSelectBody().accept(new SelectClauseVisitor(result));
                }
            });
            plainSelect.getSelectItems().forEach(si -> si.accept(visitor));
            
            Expression where = plainSelect.getWhere();
            if (where != null) {
                where.accept(new WhereExpressionVisitor(result, aliases, tableNameBuilder.toString()));
            }
        }
    }
    
    public static class WhereExpressionVisitor extends ExpressionVisitorAdapter {
        private SqlParseResult result;
        private Map<String, String> aliases;
        private String tableName;
        public WhereExpressionVisitor(SqlParseResult result, Map<String, String> aliases, String tableName) {
            this.result = result;
            this.aliases = aliases;
            this.tableName = tableName;
        }

        @Override
        public void visit(InExpression expr) {
            // TODO
        }
        
        @Override
        public void visit(EqualsTo expr) {
            // only handle String fields for now
            if (expr.getRightExpression() instanceof StringValue) {
                StringValue valueWrapper = (StringValue) expr.getRightExpression();
                String value = valueWrapper.getValue();
                
                if (expr.getLeftExpression() instanceof Column) {
                    Column column = (Column) expr.getLeftExpression();
                    String columnName = column.getColumnName();
                    if (aliases.containsKey(columnName)) {
                        columnName = aliases.get(columnName);
                    }
                    String currentTableName = tableName;
                    if (column.getTable() != null && column.getTable().getName() != null) {
                        currentTableName = column.getTable().getName();
                    }
                    if (aliases.containsKey(currentTableName)) {
                        currentTableName = aliases.get(currentTableName); 
                    }
                    result.getWhereColumns().add(new TableColumn(currentTableName, columnName, value));
                }
                
            }
            // TODO MySQL in standard mode uses " for strings and not for objects as in ANSI_SQL mode, so handle that
        }
    }
    
    public static class SqlParseResult {
        private List<TableColumn> columns = new ArrayList<>();
        private List<TableColumn> whereColumns = new ArrayList<>();
        
        public List<TableColumn> getColumns() {
            return columns;
        }
        public void setColumns(List<TableColumn> columns) {
            this.columns = columns;
        }
        public List<TableColumn> getWhereColumns() {
            return whereColumns;
        }
        public void setWhereColumns(List<TableColumn> whereColumns) {
            this.whereColumns = whereColumns;
        }
    }
    
    public static class TableColumn {
        private String columName;
        private String tableName;
        private String value;
        
        public TableColumn(String tableName, String columName, String value) {
            this.tableName = tableName;
            this.columName = columName;
            this.value = value;
        }
        
        public String getColumName() {
            return columName;
        }
        public void setColumName(String columName) {
            this.columName = columName;
        }
        public String getTableName() {
            return tableName;
        }
        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "TableColumn [columName=" + columName + ", tableName=" + tableName + ", value=" + value + "]";
        }
    }
}
