package com.logsentinel.sentineldb;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.logsentinel.sentineldb.SqlParser.SqlParseResult;
import com.logsentinel.sentineldb.SqlParser.TableColumn;

public class SqlParserTest {

    private SqlParser parser = createSqlParser();
    
    private static SqlParser createSqlParser() {
        TableMetadata tableMetadata = new TableMetadata();
        tableMetadata.setTables(Arrays.asList("table"));
        Map<String, String> idColumns = new HashMap<>();
        idColumns.put("table", "id");
        tableMetadata.setIdColumns(idColumns);
        return new SqlParser(tableMetadata);
    }
    
    @Mock
    private Connection connection;
    
    @Before
    public void setUp() throws SQLException {
        MockitoAnnotations.initMocks(this);
        Statement mockStatement = mock(Statement.class);
        ResultSet mockResultSet = mock(ResultSet.class);
        when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
        when(connection.createStatement()).thenReturn(mockStatement);
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
    public void selectWithLikeTest() {
        SqlParseResult result = parser.parse("SELECT column2 FROM table WHERE column2 LIKE '%foo%'", connection);
        assertThat(getList(result.getWhereColumns(), TableColumn::getColumName), hasItems("column2"));
        assertThat(getList(result.getWhereColumns(), TableColumn::getValue), hasItems("%foo%"));
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
        assertThat(result.getIds().iterator().next(), equalTo(2L));
    }
    
    @Test
    public void insertWithNullTest() {
        String query = "insert query insert into owners (id, first_name, last_name, address, city, telephone) values (null, ?, ?, ?, ?, ?)";
        parser.parse(query, connection);
    }
    
    public List<String> getList(List<TableColumn> columns, Function<TableColumn, String> supplierFunction) {
        return columns.stream().map(supplierFunction).collect(Collectors.toList());
    }
}
