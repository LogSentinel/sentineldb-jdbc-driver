package com.logsentinel.sentineldb;

import java.util.ArrayList;
import java.util.List;
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
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectVisitorAdapter;
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
        ((Select) stm).getSelectBody().accept(new SelectClauseVisitor(result));
        return result;
        
    }

    public SqlParseResult handleUpdate(Statement stm) {
        SqlParseResult result = new SqlParseResult();
        
        Update update = (Update) stm;
        List<Column> columns = update.getColumns();
        result.getColumns().addAll(columns.stream().map(Column::getColumnName).collect(Collectors.toList()));
        
        List<Expression> expressions = update.getExpressions();
        List<String> values = new ArrayList<>();
        for (Expression expr : expressions) {
            if (expr instanceof StringValue) {
                values.add(((StringValue) expr).getValue()); 
            }
        }
        
        update.getWhere().accept(new WhereExpressionVisitor(result));
        
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
        
        result.setColumns(columns.stream().map(Column::getColumnName).collect(Collectors.toList()));
        result.setValues(values);
        return result;
    }
    
    public static class SelectClauseVisitor extends SelectVisitorAdapter {
        private SqlParseResult result;
        public SelectClauseVisitor(SqlParseResult result) {
            this.result = result;
        }

        @Override
        public void visit(PlainSelect plainSelect) {
            plainSelect.getWhere().accept(new WhereExpressionVisitor(result));
        }
    }
    
    public static class WhereExpressionVisitor extends ExpressionVisitorAdapter {
        private SqlParseResult result;
        public WhereExpressionVisitor(SqlParseResult result) {
            this.result = result;
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
                result.getWhereColumns().add(value);
                
                if (expr.getLeftExpression() instanceof Column) {
                    Column column = (Column) expr.getLeftExpression();
                    String columnName = column.getColumnName();
                    result.getWhereColumns().add(columnName);
                }
            }
        }
    }
    
    public static class SqlParseResult {
        private List<String> columns = new ArrayList<>();
        private List<String> values = new ArrayList<>();
        private List<String> whereColumns = new ArrayList<>();
        private List<String> whereValues = new ArrayList<>();
        
        public List<String> getColumns() {
            return columns;
        }
        public void setColumns(List<String> columns) {
            this.columns = columns;
        }
        public List<String> getValues() {
            return values;
        }
        public void setValues(List<String> values) {
            this.values = values;
        }
        public List<String> getWhereColumns() {
            return whereColumns;
        }
        public void setWhereColumns(List<String> whereColumns) {
            this.whereColumns = whereColumns;
        }
        public List<String> getWhereValues() {
            return whereValues;
        }
        public void setWhereValues(List<String> whereValues) {
            this.whereValues = whereValues;
        }
    }
}
