package com.logsentinel.sentineldb;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import com.logsentinel.sentineldb.SqlParser.SqlParseResult;

public class SqlParserTest {

    @Test
    public void selectTest() {
        SqlParser parser = new SqlParser();
        SqlParseResult result = parser.parse("SELECT * FROM table WHERE column1=1 AND column2='foo'");
        System.out.println(result.getWhereColumns());
        assertThat(result.getWhereColumns(), hasItems("column2"));
        assertThat(result.getWhereValues(), hasItems("foo"));
        assertThat(result.getColumns().isEmpty(), equalTo(true));
    }
    
    @Test
    public void selectWithAliasTest() {
        SqlParser parser = new SqlParser();
        SqlParseResult result = parser.parse("SELECT column2 AS alias FROM table WHERE alias='foo'");
        assertThat(result.getWhereColumns(), hasItems("column2"));
        assertThat(result.getWhereValues(), hasItems("foo"));
    }
    
    @Test
    public void selectWithSubselectTest() {
        
    }
}
