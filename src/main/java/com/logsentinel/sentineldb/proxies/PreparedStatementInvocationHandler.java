package com.logsentinel.sentineldb.proxies;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.logsentinel.sentineldb.AuditLogService;
import com.logsentinel.sentineldb.ExternalEncryptionService;
import com.logsentinel.sentineldb.LookupManager;
import com.logsentinel.sentineldb.ResultUtils;
import com.logsentinel.sentineldb.SqlParser;
import com.logsentinel.sentineldb.SqlParser.SqlParseResult;
import com.logsentinel.sentineldb.SqlParser.TableColumn;

public class PreparedStatementInvocationHandler implements InvocationHandler {
    private PreparedStatement preparedStatement;
    private String query;
    private ExternalEncryptionService encryptionService;
    private AuditLogService auditLogService;
    private LookupManager lookupManager;
    private SqlParseResult parseResult;
    private List<TableColumn> indexedParamColumns;
    
    public PreparedStatementInvocationHandler(PreparedStatement preparedStatement, String query, 
            ExternalEncryptionService encryptionService, AuditLogService auditLogService, 
            SqlParser sqlParser, LookupManager lookupManager) throws SQLException {
        this.preparedStatement = preparedStatement;
        this.query = query;
        this.encryptionService = encryptionService;
        this.auditLogService = auditLogService;
        this.lookupManager = lookupManager;
        this.parseResult = sqlParser.parse(query, preparedStatement.getConnection());
        extractIndexedParamColumnNames();
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Object result = null;
        try {
            if (method.getName().equals("setString")) {
                TableColumn column = indexedParamColumns.get((int) args[0]);
                // in case the parameter is in the where clause, set the value to the lookup key
                // otherwise (e.g. UPDATE table SET x=?), set it to the encrypted value
                if (column.isWhereClause()) {
                    args[1] = encryptionService.getLookupKey((String) args[1]);
                } else {
                    args[1] = encryptionService.encryptString((String) args[1], 
                            column.getTableName(), 
                            column.getColumName(), 
                            UUID.randomUUID()); // check extended comment in StatementInvocationHandler
                }
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
    
    private void extractIndexedParamColumnNames() {
        indexedParamColumns = new ArrayList<>();
        List<TableColumn> allColumns = new ArrayList<>();
        allColumns.addAll(parseResult.getColumns());
        allColumns.addAll(parseResult.getWhereColumns());
        indexedParamColumns.add(null); // add an empty zeroth element
        for (TableColumn column : allColumns) {
            if (column.getValue().equals("?")) {
                indexedParamColumns.add(column);
            }
        }
    }
}
