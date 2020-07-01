package com.logsentinel.sentineldb;

import static org.mockito.Mockito.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertThat;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.logsentinel.sentineldb.SqlParser.SqlParseResult;
import com.logsentinel.sentineldb.SqlParser.TableColumn;

public class SqlParserTest {

    private SqlParser parser = new SqlParser(Arrays.asList("table"));
    
    @Mock
    private Connection connection;
    
    @Before
    public void setUp() throws SQLException {
        MockitoAnnotations.initMocks(this);
        Statement mockStatement = mock(Statement.class);
        ResultSet mockResultSet = mock(ResultSet.class);
        when(mockResultSet.next()).thenReturn(true).thenReturn(false);
        when(mockResultSet.getString(eq("Key"))).thenReturn("PRI");
        when(mockResultSet.getString(eq("Field"))).thenReturn("id");
        when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
        when(mockStatement.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(mockStatement);
        DatabaseMetaData dbMetadata = mock(DatabaseMetaData.class);
        when(dbMetadata.getDatabaseProductName()).thenReturn(DatabaseType.MYSQL.getProviderName());
        when(connection.getMetaData()).thenReturn(dbMetadata);
    }
    
    @Test
    public void selectTest() {
        SqlParseResult result = parser.parse("SELECT * FROM table WHERE column1=1 AND column2='foo'", connection);
        assertThat(getList(result.getWhereColumns(), TableColumn::getColumName), hasItems("column2"));
        assertThat(getList(result.getWhereColumns(), TableColumn::getValue), hasItems("foo"));
        assertThat(getList(result.getWhereColumns(), TableColumn::getTableName), hasItems("table"));
        assertThat(result.getColumns().isEmpty(), equalTo(true));
    }
    
    @Test
    public void selectWithAliasTest() {
        SqlParseResult result = parser.parse("SELECT column2 AS alias FROM table WHERE alias='foo'", connection);
        assertThat(getList(result.getWhereColumns(), TableColumn::getColumName), hasItems("column2"));
        assertThat(getList(result.getWhereColumns(), TableColumn::getValue), hasItems("foo"));
    }

    @Test
    public void selectWithSubselectsTest() {
        SqlParseResult result = parser.parse("SELECT * FROM (SELECT * FROM table WHERE subcolumn='foo') AS f JOIN table2 ON f.id = table2.someId", connection);
        assertThat(getList(result.getWhereColumns(), TableColumn::getColumName), hasItems("subcolumn"));
        assertThat(getList(result.getWhereColumns(), TableColumn::getValue), hasItems("foo"));
        assertThat(getList(result.getWhereColumns(), TableColumn::getTableName), hasItems("table"));
    }
    
    @Test
    public void selectWithAliasedJoinsTest() {
        SqlParseResult result = parser.parse("SELECT * FROM table JOIN table2 as t2 ON table.id = table2.other_id WHERE table.column1='c1' AND t2.column2='c2'", connection);
        assertThat(getList(result.getWhereColumns(), TableColumn::getColumName), hasItems("column1", "column2"));
        assertThat(getList(result.getWhereColumns(), TableColumn::getValue), hasItems("c1", "c2"));
        assertThat(getList(result.getWhereColumns(), TableColumn::getTableName), hasItems("table", "table2"));
        assertThat(result.getColumns().isEmpty(), equalTo(true));
    }
    
    @Test
    public void insertTest() {
        SqlParseResult result = parser.parse("INSERT INTO table (col1, col2) VALUES ('val1', 'val2')", connection);
        assertThat(getList(result.getColumns(), TableColumn::getColumName), hasItems("col1", "col2"));
        assertThat(getList(result.getColumns(), TableColumn::getValue), hasItems("val1", "val2"));
        assertThat(getList(result.getColumns(), TableColumn::getTableName), hasItems("table", "table"));
        assertThat(result.getWhereColumns().isEmpty(), equalTo(true));
    }
    
    @Test
    public void updateTest() {
        SqlParseResult result = parser.parse("UPDATE table SET col1='val1' WHERE col2='val2'", connection);
        assertThat(getList(result.getColumns(), TableColumn::getColumName), hasItems("col1"));
        assertThat(getList(result.getColumns(), TableColumn::getValue), hasItems("val1"));
        assertThat(getList(result.getColumns(), TableColumn::getTableName), hasItems("table"));
        
        assertThat(getList(result.getWhereColumns(), TableColumn::getColumName), hasItems("col2"));
        assertThat(getList(result.getWhereColumns(), TableColumn::getValue), hasItems("val2"));
        assertThat(getList(result.getWhereColumns(), TableColumn::getTableName), hasItems("table"));
    }
    
    @Test
    public void updateExtractIdTest() {
        SqlParseResult result = parser.parse("UPDATE table SET col1='val1' WHERE id=2", connection);
        assertThat(result.getId(), equalTo(2L));
    }
    
    
    public List<String> getList(List<TableColumn> columns, Function<TableColumn, String> supplierFunction) {
        return columns.stream().map(supplierFunction).collect(Collectors.toList());
    }
}
