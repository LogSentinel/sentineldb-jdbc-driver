package com.logsentinel.sentineldb;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import com.logsentinel.sentineldb.SqlParser.SqlParseResult;

public class PreparedStatementInvocationHandler implements InvocationHandler {
    private PreparedStatement preparedStatement;
    private String query;
    private ExternalEncryptionService encryptionService;
    private AuditLogService auditLogService;
    private SqlParseResult parseResult;
    
    public PreparedStatementInvocationHandler(PreparedStatement preparedStatement, String query, 
            ExternalEncryptionService encryptionService, AuditLogService auditLogService, SqlParser sqlParser) {
        this.preparedStatement = preparedStatement;
        this.query = query;
        this.encryptionService = encryptionService;
        this.auditLogService = auditLogService;
        this.parseResult = sqlParser.parse(query);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Object result = null;
        try {
            if (method.getName().equals("setString")) {
                
            }
            result = method.invoke(preparedStatement, args);
        } finally {
            if (query != null && method.getName().startsWith("execute")) {
                if (result instanceof ResultSet) {
                    List<String> columnNames = ResultUtils.getColumns((ResultSet) result);
                    auditLogService.logQuery(query, columnNames);
                    
                    // wrapping the result in a decrypting proxy
                    return Proxy.newProxyInstance(getClass().getClassLoader(), 
                            new Class[] { ResultSet.class },
                            new DecryptingResultSetInvocationHandler((ResultSet) result, encryptionService));
                } else {
                    auditLogService.logQuery(query);
                }
            }
        }
        return result;
    }
}
