package com.logsentinel.sentineldb;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import com.logsentinel.sentineldb.SqlParser.SqlParseResult;

public class StatementInvocationHandler implements InvocationHandler {
    private Statement statement;
    private ExternalEncryptionService encryptionService;
    private AuditLogService auditLogService;
    private SqlParser sqlParser;
    
    StatementInvocationHandler(Statement statement, ExternalEncryptionService encryptionService, 
            AuditLogService auditLogService, SqlParser sqlParser) {
        this.statement = statement;
        this.encryptionService = encryptionService;
        this.auditLogService = auditLogService;
        this.sqlParser = sqlParser;
    }
    
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String query = null;
        if (method.getName().equals("addBatch") || method.getName().equals("executeUpdate") || method.getName().equals("executeQuery")) {
            query = (String) args[0];
            SqlParseResult parseResult = sqlParser.parse(query);
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