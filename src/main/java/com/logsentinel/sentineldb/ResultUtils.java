package com.logsentinel.sentineldb;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ResultUtils {
    public static List<String> getColumns(ResultSet result) throws SQLException {
        ResultSetMetaData metadata = result.getMetaData();
        List<String> columnNames = new ArrayList<>();
        for (int i = 1; i <= metadata.getColumnCount(); i++) {
            columnNames.add(metadata.getColumnName(i));
        }
        return columnNames;
    }
}
