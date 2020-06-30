package com.logsentinel.sentineldb;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;

public class ConnectionInvocationHandler implements InvocationHandler {
    private Connection connection;
    private ExternalEncryptionService encryptionService;
    private AuditLogService auditLogService;
    private SqlParser sqlParser;
    
    ConnectionInvocationHandler(Connection connection, ExternalEncryptionService encryptionService, 
            AuditLogService auditLogService, SqlParser sqlParser) {
        this.connection = connection;
        this.encryptionService = encryptionService;
        this.auditLogService = auditLogService;
        this.sqlParser = sqlParser;
    }
    
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Object result = method.invoke(connection, args);
        if (method.getReturnType() == Statement.class) {
            return Proxy.newProxyInstance(getClass().getClassLoader(), 
                    new Class[] { Statement.class }, 
                    new StatementInvocationHandler((Statement) result, encryptionService, auditLogService, sqlParser));
        } else if (method.getReturnType() == PreparedStatement.class || method.getReturnType() == CallableStatement.class) {
            return Proxy.newProxyInstance(getClass().getClassLoader(), 
                    new Class[] { method.getReturnType() },
                    new PreparedStatementInvocationHandler((PreparedStatement) result, (String) args[0], 
                            encryptionService, auditLogService, sqlParser));
        }
        return result;
    }
}