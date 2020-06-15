package com.logsentinel.sentineldb;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

public class PreparedStatementInvocationHandler implements InvocationHandler {
    private PreparedStatement preparedStatement;
    private String query;
    private AuditLogService auditLogService;

    public PreparedStatementInvocationHandler(PreparedStatement preparedStatement, String query, AuditLogService auditLogService) {
        this.preparedStatement = preparedStatement;
        this.query = query;
        this.auditLogService = auditLogService;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Object result = null;
        try {
            result = method.invoke(preparedStatement, args);
        } finally {
            if (query != null && method.getName().startsWith("execute")) {
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
