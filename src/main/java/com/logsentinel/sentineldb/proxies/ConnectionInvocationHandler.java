package com.logsentinel.sentineldb.proxies;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;

import com.logsentinel.sentineldb.AuditLogService;
import com.logsentinel.sentineldb.ExternalEncryptionService;
import com.logsentinel.sentineldb.LookupManager;
import com.logsentinel.sentineldb.SqlParser;

public class ConnectionInvocationHandler implements InvocationHandler {
    private Connection connection;
    private ExternalEncryptionService encryptionService;
    private AuditLogService auditLogService;
    private SqlParser sqlParser;
    private LookupManager lookupManager;
    
    ConnectionInvocationHandler(Connection connection, ExternalEncryptionService encryptionService, 
            AuditLogService auditLogService, SqlParser sqlParser, LookupManager lookupManager) {
        this.connection = connection;
        this.encryptionService = encryptionService;
        this.auditLogService = auditLogService;
        this.sqlParser = sqlParser;
        this.lookupManager = lookupManager;
    }
    
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Object result = method.invoke(connection, args);
        if (method.getReturnType() == Statement.class) {
            return Proxy.newProxyInstance(getClass().getClassLoader(), 
                    new Class[] { Statement.class }, 
                    new StatementInvocationHandler((Statement) result, encryptionService, auditLogService, 
                            sqlParser, lookupManager));
        } else if (method.getReturnType() == PreparedStatement.class || method.getReturnType() == CallableStatement.class) {
            return Proxy.newProxyInstance(getClass().getClassLoader(), 
                    new Class[] { method.getReturnType() },
                    new PreparedStatementInvocationHandler((PreparedStatement) result, (String) args[0], 
                            encryptionService, auditLogService, sqlParser, lookupManager));
        }
        return result;
    }
}