package com.logsentinel.sentineldb;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
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
import com.logsentinel.sentineldb.proxies.ConnectionInvocationHandler;

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
    public void aliasLowercaseLikeTest() {
        String query = "select distinct owner0_.id as id1_0_0_, pets1_.id as id1_1_1_, owner0_.first_name as first_na2_0_0_, owner0_.last_name as last_nam3_0_0_, owner0_.address as address4_0_0_, owner0_.city as city5_0_0_, owner0_.telephone as telephon6_0_0_, pets1_.name as name2_1_1_, pets1_.birth_date as birth_da3_1_1_, pets1_.owner_id as owner_id4_1_1_, pets1_.type_id as type_id5_1_1_, pets1_.owner_id as owner_id4_1_0__, pets1_.id as id1_1_0__ from owners owner0_ left outer join pets pets1_ on owner0_.id=pets1_.owner_id WHERE owner0_.last_name like ?";
        SqlParseResult result = parser.parse(query, connection);
        assertThat(result.getWhereColumns().stream().anyMatch(c -> c.getColumName().equals("last_name")), equalTo(true));
    }
    
    @Test
    public void insertWithNullTest() {
        String query = "insert into owners (id, first_name, last_name, address, city, telephone) values (null, ?, ?, ?, ?, ?)";
        SqlParseResult result = parser.parse(query, connection);
        assertThat(getList(result.getColumns(), TableColumn::getColumName), hasItems("id", "first_name", "last_name", "address", "city", "telephone"));
        assertThat(getList(result.getColumns(), TableColumn::getValue), hasItems(null, "?", "?", "?", "?"));
        assertThat(getList(result.getColumns(), TableColumn::getTableName), hasItems("owners"));
    }
    
    @Test
    public void queryPreparsingTest() {
        String query = "select distinct owner0_.id as id1_0_0_, pets1_.id as id1_1_1_, owner0_.first_name as first_na2_0_0_, owner0_.last_name as last_nam3_0_0_, owner0_.address as address4_0_0_, owner0_.city as city5_0_0_, owner0_.telephone as telephon6_0_0_, pets1_.name as name2_1_1_, pets1_.birth_date as birth_da3_1_1_, pets1_.owner_id as owner_id4_1_1_, pets1_.type_id as type_id5_1_1_, pets1_.owner_id as owner_id4_1_0__, pets1_.id as id1_1_0__ from owners owner0_ left outer join pets pets1_ on owner0_.id=pets1_.owner_id WHERE owner0_.last_name like ?";
        ExternalEncryptionService encryptionService = mock(ExternalEncryptionService.class);
        when(encryptionService.getSearchableEncryptedColumns(any())).thenReturn(Collections.singletonList("last_name"));
        ConnectionInvocationHandler handler = new ConnectionInvocationHandler(connection, encryptionService, null, parser, null);
        Object[] args = new Object[] {query};
        handler.handleQueryModifications(args);
        assertThat(args[0].toString().endsWith("owner0_.last_name_sentineldb_lookup like ?"), equalTo(true));
    }
    
    public List<String> getList(List<TableColumn> columns, Function<TableColumn, String> supplierFunction) {
        return columns.stream().map(supplierFunction).collect(Collectors.toList());
    }
}
