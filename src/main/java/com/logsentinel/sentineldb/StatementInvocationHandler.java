package com.logsentinel.sentineldb;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.tuple.Pair;

import com.logsentinel.sentineldb.SqlParser.SqlParseResult;
import com.logsentinel.sentineldb.SqlParser.TableColumn;

public class StatementInvocationHandler implements InvocationHandler {
    private Statement statement;
    private ExternalEncryptionService encryptionService;
    private AuditLogService auditLogService;
    private SqlParser sqlParser;
    private LookupManager lookupManager;
    
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
            UUID encryptionRecordId = UUID.randomUUID();
            
            // TODO set the sentineldb_record_id column
            for (TableColumn column : parseResult.getColumns()) {
                if (encryptionService.isEncrypted(column.getTableName(), column.getColumName())) {
                    Pair<String, List<String>> result = encryptionService.encryptString(query, column.getTableName(), column.getColumName(), encryptionRecordId);
                    query = query.replace(column.getValue(), result.getLeft());
                    lookupManager.storeLookup(result.getRight(), column.getTableName(), column.getColumName(), statement.getConnection());
                }
            }

            for (TableColumn whereColumn : parseResult.getWhereColumns()) {
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