package com.logsentinel.sentineldb;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;

public class StatementInvocationHandler implements InvocationHandler {
    private Statement statement;
    private ExternalEncryptionService encryptionService;
    private AuditLogService auditLogService;
    
    StatementInvocationHandler(Statement statement, ExternalEncryptionService encryptionService, 
            AuditLogService auditLogService) {
        this.statement = statement;
        this.encryptionService = encryptionService;
        this.auditLogService = auditLogService;
    }
    
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String query = null;
        if (method.getName().equals("addBatch") || method.getName().equals("executeUpdate") || method.getName().equals("executeQuery")) {
            query = (String) args[0];
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