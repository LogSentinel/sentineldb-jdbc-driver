package com.logsentinel.sentineldb;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public enum DatabaseType {
    
    MYSQL("MySQL"),
    MARIADB("MariaDB"),
    POSTGRESQL("PostgreSQL"),
    ORACLE("Oracle"),
    MS_SQL_SERVER("Microsoft SQL Server"),
    INFORMIX("Informix Dynamic Server"),
    DB2("DB2"),
    SYBASE("Sybase Anywhere"),
    SQLITE("SQLite"),
    H2("H2");
    // TODO Firebird, Ingres, Apache Derby, H2
    
    private String providerName;
    private static final Map<String, DatabaseType> mapping = new HashMap<>();

    static {
        Arrays.stream(DatabaseType.values()).forEach(dt -> mapping.put(dt.providerName, dt));
    }
    
    private DatabaseType(String providerName) {
        this.providerName = providerName;
    }
    
    public static DatabaseType findByName(String providerName) {
        return mapping.get(providerName);
    }

    public String getProviderName() {
        return providerName;
    }
}
