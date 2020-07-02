package com.logsentinel.sentineldb;

import java.util.List;
import java.util.Map;

/**
 * Holding multiple representations of table metadata
 */
public class TableMetadata {

    private List<String> tables;
    private Map<String, List<String>> tableColumns;
    private Map<String, String> idColumns;
    public List<String> getTables() {
        return tables;
    }
    public void setTables(List<String> tables) {
        this.tables = tables;
    }
    public Map<String, List<String>> getTableColumns() {
        return tableColumns;
    }
    public void setTableColumns(Map<String, List<String>> tableColumns) {
        this.tableColumns = tableColumns;
    }
    public Map<String, String> getIdColumns() {
        return idColumns;
    }
    public void setIdColumns(Map<String, String> idColumns) {
        this.idColumns = idColumns;
    }
}
