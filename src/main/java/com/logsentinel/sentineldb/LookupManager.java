package com.logsentinel.sentineldb;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * Manages the lookup table. The lookup table has lookup_key=hash(encrypt(plaintext)) and 
 * target_id = the target id for each row in a table that has sensitive data.
 * 
 * target_id is obtained in the following ways:
 * - by getting the last inserted ID in a database-specific way, in case of generated IDs
 * - by getting the table definition and getting the ID column(s) and their insert values
 * - for composite-key tables, the key is concatenated
 * 
 */
public class LookupManager {

    public static final String SENTINELDB_LOOKUP_COLUMN_SUFFIX = "_sentineldb_lookup";
    public static final String SENTINELDB_RECORD_ID_COLUMN_NAME = "sentineldb_record_id";
    
    private ExternalEncryptionService encryptionService;
    private Connection connection;
    
    public LookupManager(ExternalEncryptionService encryptionService, Connection connection) { 
        this.encryptionService = encryptionService;
        this.connection = connection;
    }
    
    public void initLookup(List<String> tables) {
        try {
            try (Statement stm = connection.createStatement()) {
                stm.executeQuery("SELECT * FROM sentineldb_lookup LIMIT 1");
            } catch (SQLException ex) {
                try (Statement createStm = connection.createStatement()) {
                    // table not found, create it
                    createStm.executeUpdate("CREATE TABLE sentineldb_lookup (lookup_key VARCHAR(44) PRIMARY KEY, target_record_id VARCHAR(36)");
                }
            }
            
            try (Statement stm = connection.createStatement()) {
                appendRecordIdColumn(tables, stm);
                appendLookupColumn(tables, stm);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    private void appendLookupColumn(List<String> tables, Statement stm) throws SQLException {
        for (String table : tables) {
            List<String> searchableColumns = encryptionService.getSearchableEncryptedColumns(table);
            for (String column : searchableColumns) {
                try {
                    stm.executeUpdate("ALTER TABLE " + table + " ADD " + column + SENTINELDB_LOOKUP_COLUMN_SUFFIX + " VARCHAR(44)");
                    stm.executeUpdate("CREATE INDEX " + column + "_sentineldb_lookup_idx ON " + table + " (" + column + SENTINELDB_LOOKUP_COLUMN_SUFFIX + ")");
                } catch (SQLException ex) {
                    // ignore failures to create column and index; it means they already exist
                }
            }
        }
    }


    private void appendRecordIdColumn(List<String> tables, Statement stm) {
        for (String table : tables) {
            if (encryptionService.tableConstainsSensitiveData(table)) {
                try {
                    stm.executeUpdate("ALTER TABLE " + table + " ADD " + SENTINELDB_RECORD_ID_COLUMN_NAME + " VARCHAR(36)");
                    stm.executeUpdate("CREATE UNIQUE INDEX sentineldb_record_id_idx ON " + table + " (" + SENTINELDB_RECORD_ID_COLUMN_NAME + ")");
                } catch (SQLException ex) {
                    // ignore failures to create column and index; it means they already exist
                }
            }
        }
    }

    public void storeLookup(List<String> lookupKeys, String table, String column, Connection connection) throws SQLException {
        try (Statement stm = connection.createStatement()) {
            
        }
    }
}
