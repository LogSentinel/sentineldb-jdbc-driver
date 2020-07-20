package com.logsentinel.sentineldb.proxies;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import com.logsentinel.sentineldb.AuditLogService;
import com.logsentinel.sentineldb.ExternalEncryptionService;
import com.logsentinel.sentineldb.LookupManager;
import com.logsentinel.sentineldb.SqlParser;
import com.logsentinel.sentineldb.SqlParser.SqlParseResult;
import com.logsentinel.sentineldb.SqlParser.TableColumn;

public class ConnectionInvocationHandler implements InvocationHandler {
    private Connection connection;
    private ExternalEncryptionService encryptionService;
    private AuditLogService auditLogService;
    private SqlParser sqlParser;
    private LookupManager lookupManager;
    
    public ConnectionInvocationHandler(Connection connection, ExternalEncryptionService encryptionService, 
            AuditLogService auditLogService, SqlParser sqlParser, LookupManager lookupManager) {
        this.connection = connection;
        this.encryptionService = encryptionService;
        this.auditLogService = auditLogService;
        this.sqlParser = sqlParser;
        this.lookupManager = lookupManager;
    }
    
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        // for prepared INSERT statements we need to add the lookup columns to be inserted together with the rest of the data
        SqlParseResult preParseResult = null;
        if (method.getReturnType() == PreparedStatement.class) {
            try {
                preParseResult = handleQueryModifications(args);
            } catch (Exception ex) {
                System.err.println("Failed to parse insert query " + args[0]);
                throw ex;
            }
        }
        
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
                            encryptionService, auditLogService, sqlParser, preParseResult, lookupManager));
        }
        return result;
    }

    public SqlParseResult handleQueryModifications(Object[] args) {
        String query = (String) args[0];
        // pre-parse the query in order to modify it before passing it to the target connection
        SqlParseResult result = sqlParser.parse(query, connection);
        if (query.toUpperCase().startsWith("INSERT")) {
            List<String> lookupColumns = encryptionService.getSearchableEncryptedColumns(result.getColumns().iterator().next().getTableName());
            // add the lookup columns to the insert. If there are no fields specified, the first replace won't change anything
            Pattern replacement = Pattern.compile("\\) VALUES", Pattern.CASE_INSENSITIVE);
            query = replacement.matcher(query).replaceFirst("," + StringUtils.join(lookupColumns
                    .stream().map(c -> c + LookupManager.SENTINELDB_LOOKUP_COLUMN_SUFFIX).iterator(), ',') + ") VALUES");
            query = query.replace("?)", "?" + StringUtils.repeat(",?", lookupColumns.size()) + ")");
            
            args[0] = query;
            return result;
        } else if (query.toUpperCase().startsWith("UPDATE")) {
            List<String> lookupColumns = encryptionService.getSearchableEncryptedColumns(result.getColumns().iterator().next().getTableName());
            Pattern replacement = Pattern.compile("SET ", Pattern.CASE_INSENSITIVE);
            query = replacement.matcher(query).replaceFirst("SET " + StringUtils.join(lookupColumns
                    .stream().map(c -> c + LookupManager.SENTINELDB_LOOKUP_COLUMN_SUFFIX + "=?").iterator(), ',') + ",");
            args[0] = query;
            return result;
        }
        
        // we have to replace the column names in the WHERE clause if lookups by encrypted values are to be used
        if (query.toUpperCase().contains(" WHERE ")) {
            // TODO handle more complicated queries with subselects and multiple WHERE clauses
            String[] parts = Pattern.compile(" WHERE ", Pattern.CASE_INSENSITIVE).split(query);
            for (TableColumn whereColumn : result.getWhereColumns()) {
                if (encryptionService.getSearchableEncryptedColumns(whereColumn.getTableName()).contains(whereColumn.getColumName())) {
                    parts[1] = parts[1].replace(whereColumn.getColumName(), whereColumn.getColumName() + LookupManager.SENTINELDB_LOOKUP_COLUMN_SUFFIX);
                }
            }
            args[0] = parts[0] + " WHERE " + parts[1];
        }
        return null;
    }
}