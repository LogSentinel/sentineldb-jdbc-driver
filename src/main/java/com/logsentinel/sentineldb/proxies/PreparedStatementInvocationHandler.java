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

import org.apache.commons.lang3.tuple.Pair;

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
    private List<TableColumn> paramColumnsByPosition;
    private List<String> searchableColumns;
    
    public PreparedStatementInvocationHandler(PreparedStatement preparedStatement, String query, 
            ExternalEncryptionService encryptionService, AuditLogService auditLogService, 
            SqlParser sqlParser, SqlParseResult preParseResult, LookupManager lookupManager) throws SQLException {
        this.preparedStatement = preparedStatement;
        this.query = query;
        this.encryptionService = encryptionService;
        this.auditLogService = auditLogService;
        this.lookupManager = lookupManager;
        
        try {
            this.parseResult = preParseResult != null ? preParseResult : sqlParser.parse(query, preparedStatement.getConnection());
            extractIndexedParamColumnNames();
        } catch (Exception ex) {
            System.err.println("Failed to parse query " + query);
            throw ex;
        }
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Object result = null;
        try {
            if (parseResult != null && method.getName().equals("setString")) {
                TableColumn column = paramColumnsByPosition.get((int) args[0]);
                // modify the value for encrypted columns as well as for lookups
                // the original query is modified prior to preparing the statement, 
                // and encrypted columns in the WHERE clause are replaced with their lookup counterparts
                if (column.getColumName().toLowerCase().endsWith(LookupManager.SENTINELDB_LOOKUP_COLUMN_SUFFIX) 
                        || encryptionService.isEncrypted(column.getTableName(), column.getColumName())) {
                    // in case the parameter is in the where clause, set the value to the lookup key
                    // otherwise (e.g. UPDATE table SET x=?), set it to the encrypted value
                    if (column.isWhereClause()) {
                        String value = normalizeValue((String) args[1]);
                        args[1] = encryptionService.getLookupKey(value);
                    } else {
                        Pair<String, List<String>> encryptionResult = encryptionService.encryptString((String) args[1], 
                                column.getTableName(), 
                                column.getColumName(), 
                                UUID.randomUUID());
                        args[1] = encryptionResult.getKey(); // check extended comment in StatementInvocationHandler

                        if ((query.toUpperCase().startsWith("INSERT") || query.toUpperCase().startsWith("UPDATE")) 
                                && encryptionResult.getValue() != null && !encryptionResult.getValue().isEmpty()) {
                            if (encryptionResult.getValue().size() > 1) {
                                // TODO extract id values that are set as prepared statement parameters
                                lookupManager.storeLookup(encryptionResult.getValue(), column.getTableName(), 
                                    column.getColumName(), parseResult.getIds(), preparedStatement.getConnection());
                            } else if (query.toUpperCase().startsWith("UPDATE")){
                                // UPDATE queries are modified with prepending the lookup columns, so we need to offset the position
                                // with the number of prepended columns, and set the appropriate lookup value
                                int idx = searchableColumns.indexOf(column.getColumName()) + 1;
                                preparedStatement.setString(idx, encryptionResult.getValue().iterator().next());
                            } else if (query.toUpperCase().startsWith("INSERT")) {
                                // INSERT queries are modified with appending the lookup columns, so no need for offsetting,
                                // we just need to set the appropriate lookup value
                                preparedStatement.setString(paramColumnsByPosition.size() + searchableColumns.indexOf(column.getColumName()), 
                                        encryptionResult.getValue().iterator().next());
                            }
                        }
                    }
                }
            }
            if (method.getName().startsWith("set")) {
                offsetParamIndex(args);
            }
            result = method.invoke(preparedStatement, args);
        } catch (Exception ex) {
            System.out.println("Exception for query " + query);
            ex.printStackTrace();
            throw ex;
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

    public String normalizeValue(String value) {
        // handle LIKE syntax
        if (value.startsWith("%")) {
            value = value.substring(1);
        }
        if (value.endsWith("%")) {
           value = value.substring(0, value.length() - 2); 
        }
        return value;
    }

    public void offsetParamIndex(Object[] args) {
        // we have to offset all columns in UPDATE queries
        if (query.startsWith("UPDATE")) {
            int position = (int) args[0];
            position += encryptionService.getSearchableEncryptedColumns(parseResult.getMainTable()).size();
            args[0] = position;
        }
    }
    
    private void extractIndexedParamColumnNames() {
        paramColumnsByPosition = new ArrayList<>();
        searchableColumns = new ArrayList<>();
        List<TableColumn> allColumns = new ArrayList<>();
        allColumns.addAll(parseResult.getColumns());
        allColumns.addAll(parseResult.getWhereColumns());
        paramColumnsByPosition.add(null); // add an empty zeroth element
        for (TableColumn column : allColumns) {
            if (column.getValue() != null && column.getValue().equals("?")) {
                paramColumnsByPosition.add(column);
            }
        }
        for (TableColumn column : parseResult.getColumns()) {
            if (encryptionService.getSearchableEncryptedColumns(column.getTableName()).contains(column.getColumName())) {
                searchableColumns.add(column.getColumName());
            }
        }
    }
}
