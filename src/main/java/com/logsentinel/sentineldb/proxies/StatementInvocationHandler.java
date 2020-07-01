package com.logsentinel.sentineldb.proxies;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.tuple.Pair;

import com.logsentinel.sentineldb.AuditLogService;
import com.logsentinel.sentineldb.ExternalEncryptionService;
import com.logsentinel.sentineldb.LookupManager;
import com.logsentinel.sentineldb.ResultUtils;
import com.logsentinel.sentineldb.SqlParser;
import com.logsentinel.sentineldb.SqlParser.SqlParseResult;
import com.logsentinel.sentineldb.SqlParser.TableColumn;

public class StatementInvocationHandler implements InvocationHandler {
    private Statement statement;
    private ExternalEncryptionService encryptionService;
    private AuditLogService auditLogService;
    private SqlParser sqlParser;
    private LookupManager lookupManager;
    private PreparedStatement setRecordId;
    
    StatementInvocationHandler(Statement statement, ExternalEncryptionService encryptionService, 
            AuditLogService auditLogService, SqlParser sqlParser, LookupManager lookupManager) {
        this.statement = statement;
        this.encryptionService = encryptionService;
        this.auditLogService = auditLogService;
        this.sqlParser = sqlParser;
        this.lookupManager = lookupManager;
    }
    
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String query = null;
        if (method.getName().equals("addBatch") || method.getName().equals("executeUpdate") || method.getName().equals("executeQuery")) {
            query = (String) args[0];
            SqlParseResult parseResult = sqlParser.parse(query, statement.getConnection());
            
            for (TableColumn column : parseResult.getColumns()) {
                // "getColumns" returns columns for INSERT and UPDATE queries
                // for them, we generate an ID every time. We don't need to preserve the recordId across updates, as the ID is used to identify the record 
                // when the key is fetched in the key-management service. It is entirely acceptable to have multiple fields in the same record with different recordIds
                // as they are individually decrypted based on the ID stored in the column itself
                // The added bonus of that approach is that is serves as re-encryption (using a new key for every update)
                // the only downside is that in update-heavy databases there will be a lot of unused keys in the key management system
                // However, keys are cheap and there can be a scheduled job that collects all active IDs and deletes dormant keys (TODO)
                
                UUID encryptionRecordId = UUID.randomUUID();
                if (encryptionService.isEncrypted(column.getTableName(), column.getColumName())) {
                    Pair<String, List<String>> result = encryptionService.encryptString(query, column.getTableName(), column.getColumName(), encryptionRecordId);
                    query = query.replace(column.getValue(), result.getLeft());
                    lookupManager.storeLookup(result.getRight(), column.getTableName(), column.getColumName(), statement.getConnection());
                }
            }

            for (TableColumn whereColumn : parseResult.getWhereColumns()) {
                // replace: /where x="y"/where x_sentineldb_lookup=hash(enc(y))/ to make queries work
                query = query.replace(whereColumn.getValue(), encryptionService.getLookupKey(whereColumn.getValue()));
                query = query.replace(whereColumn.getColumName(), whereColumn.getColumName() + LookupManager.SENTINELDB_LOOKUP_COLUMN_SUFFIX);
            }
            args[0] = query;
        }
        Object result = null;
        
        try {
            result = method.invoke(statement, args);
        } finally {
            if (query != null) {
                if (result instanceof ResultSet) {
                    ResultSet resultSet = (ResultSet) result;
                    List<String> columnNames = ResultUtils.getColumns(resultSet);
                    auditLogService.logQuery(query, columnNames);
                    
                    // wrapping the result in a decrypting proxy
                    return Proxy.newProxyInstance(getClass().getClassLoader(), 
                            new Class[] { ResultSet.class },
                            new DecryptingResultSetInvocationHandler(resultSet, encryptionService));
                } else {
                    auditLogService.logQuery(query);
                }
            }
        }
        return result;
    }
    
}