package com.logsentinel.sentineldb;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import com.logsentinel.sentineldb.SqlParser.SqlParseResult;

public class SqlParserTest {

    @Test
    public void selectTests() {
        SqlParser parser = new SqlParser();
        SqlParseResult result = parser.parse("SELECT * FROM table WHERE column1=1 AND column2=\"foo\"");
        assertThat(result.getWhereColumns(), hasItems("column2"));
        assertThat(result.getWhereValues(), hasItems("foo"));
        assertThat(result.getColumns().isEmpty(), equalTo(true));
    }
}
