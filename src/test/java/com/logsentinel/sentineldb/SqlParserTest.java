package com.logsentinel.sentineldb;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertThat;

import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.junit.Test;

import com.logsentinel.sentineldb.SqlParser.SqlParseResult;
import com.logsentinel.sentineldb.SqlParser.TableColumn;

public class SqlParserTest {

    @Test
    public void selectTest() {
        SqlParser parser = new SqlParser();
        SqlParseResult result = parser.parse("SELECT * FROM table WHERE column1=1 AND column2='foo'");
        assertThat(getList(result.getWhereColumns(), TableColumn::getColumName), hasItems("column2"));
        assertThat(getList(result.getWhereColumns(), TableColumn::getValue), hasItems("foo"));
        assertThat(getList(result.getWhereColumns(), TableColumn::getTableName), hasItems("table"));
        assertThat(result.getColumns().isEmpty(), equalTo(true));
    }
    
    @Test
    public void selectWithAliasTest() {
        SqlParser parser = new SqlParser();
        SqlParseResult result = parser.parse("SELECT column2 AS alias FROM table WHERE alias='foo'");
        assertThat(getList(result.getWhereColumns(), TableColumn::getColumName), hasItems("column2"));
        assertThat(getList(result.getWhereColumns(), TableColumn::getValue), hasItems("foo"));
    }

    @Test
    public void selectWithSubselectsTest() {
        SqlParser parser = new SqlParser();
        SqlParseResult result = parser.parse("SELECT * FROM (SELECT * FROM table WHERE subcolumn='foo') AS f JOIN table2 ON f.id = table2.someId");
        assertThat(getList(result.getWhereColumns(), TableColumn::getColumName), hasItems("subcolumn"));
        assertThat(getList(result.getWhereColumns(), TableColumn::getValue), hasItems("foo"));
        assertThat(getList(result.getWhereColumns(), TableColumn::getTableName), hasItems("table"));
    }
    
    public List<String> getList(List<TableColumn> columns, Function<TableColumn, String> supplierFunction) {
        return columns.stream().map(supplierFunction).collect(Collectors.toList());
    }
}
