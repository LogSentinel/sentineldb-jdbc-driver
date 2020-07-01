package com.logsentinel.sentineldb;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;

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

    public void initLookup(Connection connection, List<String> tables) {
        try {
            Statement stm = connection.createStatement();
            try {
                stm.executeQuery("SELECT * FROM sentineldb_lookup LIMIT 1");
            } catch (SQLException ex) {
                // table not found, create it
                stm.executeUpdate("CREATE TABLE sentineldb_lookup (lookup_key VARCHAR(200) PRIMARY KEY, target_id VARCHAR(100), target_id_type VARCHAR(5)");
            }
            
            appendRecordIdColumn(tables, connection);
            appendLookupColumn(tables, connection);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    private void appendLookupColumn(List<String> tables, Connection connection) {
        // TODO Auto-generated method stub
    }


    private void appendRecordIdColumn(List<String> tables, Connection connection) {
        // TODO Auto-generated method stub
        
    }

    public void storeLookup(String lookupKey, UUID value) {
        
    }
    
    public static enum IdType {
        INT, UUID, STRING, COMPOSITE
    }
}
