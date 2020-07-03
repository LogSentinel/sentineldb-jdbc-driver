package com.logsentinel.sentineldb;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ArrayUtils;

import com.logsentinel.sentineldb.proxies.ConnectionInvocationHandler;

/**
 * A driver proxy that gets registered by simply adding the sentineldb: prefix in the connection string, e.g.:
 * jdbc:sentineldb:mysql://HOST
 * jdbc:sentineldb:postgresql://HOST
 * jdbc:sentineldb:microsoft:sqlserver://HOST 
 * 
 * Parameters to be specified include authentication details for Trails 
 * as well as a fully-qualified static method name (com.company.Class::method) for extraction of the current actor 
 * The method should take no arguments and return a String[] with result[0]=actorId and optionally result[1]=actorDisplayName
 *
 */
public class SentinelDBDriver implements Driver {
    
    private static final String SENTINELDB_ORGANIZATION_ID = "sentineldbOrganizationId";
    private static final String SENTINELDB_SECRET = "sentineldbSecret";
    private static final String SENTINELDB_DATASTORE_ID = "sentineldbDatastoreId";
    private static final String ACTOR_EXTRACTION_FUNCTION = "actorExtractionFunction";
    private static final String TRAILS_APPLICATION_ID = "trailsApplicationId";
    private static final String TRAILS_SECRET = "trailsSecret";
    private static final String TRAILS_ORGANIZATION_ID = "trailsOrganizationId";
    private static final String TRAILS_URL = "trailsUrl";
    private static final int MAJOR_VERSION = 1;
    private static final int MINOR_VERSION = 0;
    
    private List<String> ALL_PROPERTIES = Arrays.asList(SENTINELDB_ORGANIZATION_ID, SENTINELDB_SECRET, SENTINELDB_DATASTORE_ID, TRAILS_ORGANIZATION_ID, 
            TRAILS_SECRET, TRAILS_APPLICATION_ID, TRAILS_URL, ACTOR_EXTRACTION_FUNCTION);

    private static final String CONNECTION_STRING_PREFIX = "sentineldb:"; 
    
    static {
        try {
            java.sql.DriverManager.registerDriver(new SentinelDBDriver());
        } catch (SQLException e) {
            throw new RuntimeException("Can't register driver!");
        }
    }
    
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        
        String delegatedUrl = url.replace(CONNECTION_STRING_PREFIX, "");
        Driver delegatedDriver = DriverManager.getDriver(delegatedUrl);
        
        String actorFunctionFQN = info.getProperty(ACTOR_EXTRACTION_FUNCTION);
        Method actorExtractionMethod = null;
        if (actorFunctionFQN != null && !actorFunctionFQN.isEmpty()) {
            try {
                String[] parts = actorFunctionFQN.split("::");
                actorExtractionMethod = Class.forName(parts[0]).getDeclaredMethod(parts[1]);
            } catch (NoSuchMethodException | SecurityException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        
        Map<String, String> urlParams = splitParams(url);
        
        AuditLogService auditLogService = new AuditLogService(getProperty(info, urlParams, TRAILS_ORGANIZATION_ID, false), 
                getProperty(info, urlParams, TRAILS_SECRET, false), 
                getProperty(info, urlParams, TRAILS_APPLICATION_ID, false),
                getProperty(info, urlParams, TRAILS_URL, false),
                actorExtractionMethod);
        auditLogService.init();
        
        String sentinelDbOrganizationId = getProperty(info, urlParams, SENTINELDB_ORGANIZATION_ID, true);
        String sentinelDbSecret = getProperty(info, urlParams, SENTINELDB_SECRET, true);
        String sentinelDbDatastoreId = getProperty(info, urlParams, SENTINELDB_DATASTORE_ID, true);
        
        ExternalEncryptionService encryptionService = new ExternalEncryptionService(
                sentinelDbOrganizationId, sentinelDbSecret,
                UUID.fromString(sentinelDbDatastoreId));
        encryptionService.init();
        
        delegatedUrl = cleanupParameters(delegatedUrl, urlParams);
        
        Connection connection = delegatedDriver.connect(delegatedUrl, info);
        
        try (Statement stm = connection.createStatement()) {
            // TODO periodically reload table data if we assume database changes can happen without an application restart?
            List<String> tables = listTables(stm);
            TableMetadata tableMetadata = new TableMetadata();
            tableMetadata.setTables(tables);
            tableMetadata.setTableColumns(listTableColumns(tables, stm));
            tableMetadata.setIdColumns(listIdColumns(tables, stm));

            LookupManager lookupManager = new LookupManager(encryptionService, connection, tableMetadata);
            lookupManager.initLookup();
            
            return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), 
                    new Class[] {Connection.class}, 
                    new ConnectionInvocationHandler(connection, encryptionService, 
                            auditLogService, new SqlParser(tableMetadata), lookupManager));
        }
    }

    private String cleanupParameters(String delegatedUrl, Map<String, String> params) {
        if (!delegatedUrl.endsWith(";")) {
            delegatedUrl = delegatedUrl + ";";
        }
        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (ALL_PROPERTIES.contains(entry.getKey())) {
                delegatedUrl = delegatedUrl.replace(entry.getKey() + "=" + entry.getValue() + ";", "");
            }
        }
        if (delegatedUrl.endsWith(";")) {
            delegatedUrl = delegatedUrl.substring(0, delegatedUrl.length() - 2);
        }
        return delegatedUrl;
        
    }

    public String getProperty(Properties info, Map<String, String> urlParams, String key, boolean required) {
        try {
            String value = info.getProperty(key);
            if (value == null) {
                value = urlParams.get(key);
            }
            if (value == null && required) {
                throw new IllegalArgumentException("Missing property " + key);
            }
            return value;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private List<String> listTables(Statement stm) throws SQLException {
        String database = stm.getConnection().getMetaData().getDatabaseProductName();
        List<String> result = new ArrayList<>();
        // if is more readable than switch
        DatabaseType databaseType = DatabaseType.findByName(database);
        if (databaseType == DatabaseType.MYSQL || databaseType == DatabaseType.MARIADB || databaseType == DatabaseType.H2) {
            ResultSet rs = stm.executeQuery("SHOW TABLES");
            while (rs.next()) {
                result.add(rs.getString(1));
            }
        } else if (databaseType == DatabaseType.POSTGRESQL) {
            ResultSet rs = stm.executeQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'");
            while (rs.next()) {
                result.add(rs.getString(1));
            }
        } // TODO
        return result;
    }
    
    private Map<String, List<String>> listTableColumns(List<String> tables, Statement stm) throws SQLException {
        String database = stm.getConnection().getMetaData().getDatabaseProductName();
        DatabaseType databaseType = DatabaseType.findByName(database);
        Map<String, List<String>> result = new HashMap<>();
        for (String tableName : tables) {
            List<String> columns = new ArrayList<>();
            result.put(tableName, columns);
            
            if (databaseType == DatabaseType.MYSQL || databaseType == DatabaseType.MARIADB) {
                ResultSet rs = stm.executeQuery("DESCRIBE " + tableName);
                while (rs.next()) {
                    columns.add(rs.getString("Field"));
                }
            } else if (databaseType == DatabaseType.POSTGRESQL) {
                
            } else if (databaseType == DatabaseType.H2) {
                ResultSet rs = stm.executeQuery("SHOW COLUMNS FROM " + tableName);
                while (rs.next()) {
                    columns.add(rs.getString("FIELD"));
                }
                
            } // TODO
        }
        return result;
    }
    
    private Map<String, String> listIdColumns(List<String> tables, Statement sqlStatement) throws SQLException {
        String database = sqlStatement.getConnection().getMetaData().getDatabaseProductName();
        DatabaseType databaseType = DatabaseType.findByName(database);
        Map<String, String> idColumns = new HashMap<>();
        for (String tableName : tables) {
            if (databaseType == DatabaseType.MYSQL || databaseType == DatabaseType.MARIADB) {
                ResultSet rs = sqlStatement.executeQuery("DESCRIBE " + tableName);
                while (rs.next()) {
                    // TODO composite IDs?
                    if ("PRI".equals(rs.getString("Key"))) {
                        idColumns.put(tableName.toLowerCase(), rs.getString("Field"));
                    }
                }
            } else if (databaseType == DatabaseType.POSTGRESQL) {
                
            } else if (databaseType == DatabaseType.H2) {
                ResultSet rs = sqlStatement.executeQuery("SHOW COLUMNS FROM " + tableName);
                while (rs.next()) {
                    // TODO composite IDs?
                    if ("PRI".equals(rs.getString("KEY"))) {
                        idColumns.put(tableName.toLowerCase(), rs.getString("FIELD"));
                    }
                }
                
            } // TODO
        }
        return idColumns;
    }
    
    @Override
    public boolean acceptsURL(String url) throws SQLException {
        if (!url.contains(CONNECTION_STRING_PREFIX)) {
            return false;
        }
        String delegatedUrl = url.replace(CONNECTION_STRING_PREFIX, "");
        Driver delegatedDriver = DriverManager.getDriver(delegatedUrl);
        if (!delegatedDriver.acceptsURL(delegatedUrl)) {
            return false;
        }
        return true;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        DriverPropertyInfo[] result = DriverManager.getDriver(url.replace(CONNECTION_STRING_PREFIX, "")).getPropertyInfo(url, info);
        DriverPropertyInfo orgIdProperty = new DriverPropertyInfo(TRAILS_ORGANIZATION_ID, null);
        DriverPropertyInfo orgSecretProperty = new DriverPropertyInfo(TRAILS_SECRET, null);
        DriverPropertyInfo appIdProperty = new DriverPropertyInfo(TRAILS_APPLICATION_ID, null);
        DriverPropertyInfo urlProperty = new DriverPropertyInfo(TRAILS_URL, null);
        DriverPropertyInfo userExtractionFunctionProperty = new DriverPropertyInfo(ACTOR_EXTRACTION_FUNCTION, null);
        DriverPropertyInfo dbOrgIdProperty = new DriverPropertyInfo(SENTINELDB_ORGANIZATION_ID, null);
        DriverPropertyInfo dbSecretProperty = new DriverPropertyInfo(SENTINELDB_SECRET, null);
        DriverPropertyInfo dbDatastoreIdProperty = new DriverPropertyInfo(SENTINELDB_DATASTORE_ID, null);
        ArrayUtils.addAll(result, orgIdProperty, orgSecretProperty, appIdProperty, urlProperty, 
                userExtractionFunctionProperty, 
                dbOrgIdProperty, dbSecretProperty, dbDatastoreIdProperty);
        return result;
    }

    @Override
    public int getMajorVersion() {
        return MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion() {
        return MINOR_VERSION;
    }

    @Override
    public boolean jdbcCompliant() {
        return true;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }
    
    public Map<String, String> splitParams(String uri) {
        if (uri == null || uri.isEmpty() || !uri.contains(";")) {
            return Collections.emptyMap();
        }
        return Arrays.stream(uri.split(";"))
                .map(this::splitParameter)
                .filter(e -> e.getKey() != null && e.getValue() != null)
                .collect(Collectors.toMap(SimpleImmutableEntry::getKey, SimpleImmutableEntry::getValue));
    }

    private SimpleImmutableEntry<String, String> splitParameter(String it) {
        final int idx = it.indexOf("=");
        final String key = idx > 0 ? it.substring(0, idx) : it;
        final String value = idx > 0 && it.length() > idx + 1 ? it.substring(idx + 1) : null;
        return new SimpleImmutableEntry<>(key, value);
    }

}
