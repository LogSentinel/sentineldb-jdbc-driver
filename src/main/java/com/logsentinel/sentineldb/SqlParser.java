package com.logsentinel.sentineldb;

import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectVisitorAdapter;

public class SqlParser {

    /**
     * cases:
     * INSERT INTO x VALUES (...);
     * INSERT INTO x (y,z) VALUES (..);
     * UPDATE x SET y=z
     * any query with WHERE x=y
     * any query with WHERE x iN (..)
     */
    
    public static void main(String[] args) {
        SqlParser parser = new SqlParser();
        parser.parse("SELECT * FROM users WHERE email='foo' AND test=2");
    }
    
    public void parse(String query) {
        try {
            Statement stm = CCJSqlParserUtil.parse(query);
            if (stm instanceof Select) {
                ((Select) stm).getSelectBody().accept(new SelectClauseVisitor());
            } else if (stm instanceof Insert) {
                Insert insert = (Insert) stm;
                List<Column> columns = ((Insert) stm).getColumns();
                // if no columns are specified, fetch from database?
                ItemsList items = insert.getItemsList();
                //items.accept(new );
            }
            
        } catch (JSQLParserException e) {
            e.printStackTrace();
        }
            
    }
    
    public static class SelectClauseVisitor extends SelectVisitorAdapter {
        @Override
        public void visit(PlainSelect plainSelect) {
            plainSelect.getWhere().accept(new WhereExpressionVisitor());
        }
    }
    
    public static class WhereExpressionVisitor extends ExpressionVisitorAdapter {
        @Override
        public void visit(EqualsTo expr) {
            if (expr.getLeftExpression() instanceof Column) {
                Column column = (Column) expr.getLeftExpression();
                String columnName = column.getColumnName();
                System.out.println(columnName);
            }
            if (expr.getRightExpression() instanceof StringValue) {
                StringValue valueWrapper = (StringValue) expr.getRightExpression();
                String value = valueWrapper.getValue();
                System.out.println(value);
            }
        }
    }
}
