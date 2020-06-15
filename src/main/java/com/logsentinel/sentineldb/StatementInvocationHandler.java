package com.logsentinel.sentineldb;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

public class StatementInvocationHandler implements InvocationHandler {
    private Statement statement;
    private AuditLogService auditLogService;
    
    StatementInvocationHandler(Statement statement, AuditLogService auditLogService) {
        this.statement = statement;
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
                    List<String> columnNames = ResultUtils.getColumns((ResultSet) result);
                    auditLogService.logQuery(query, columnNames);
                } else {
                    auditLogService.logQuery(query);
                }
            }
        }
        return result;
    }
}