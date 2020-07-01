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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Logger;

import org.apache.commons.lang3.ArrayUtils;

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
        
        AuditLogService auditLogService = new AuditLogService(info.getProperty(TRAILS_ORGANIZATION_ID), 
                info.getProperty(TRAILS_SECRET), 
                info.getProperty(TRAILS_APPLICATION_ID),
                info.getProperty(TRAILS_URL),
                actorExtractionMethod);
        auditLogService.init();
        
        ExternalEncryptionService encryptionService = new ExternalEncryptionService(
                info.getProperty(SENTINELDB_ORGANIZATION_ID), 
                info.getProperty(SENTINELDB_SECRET),
                UUID.fromString(info.getProperty(SENTINELDB_DATASTORE_ID)));
        encryptionService.init();
        
        Connection connection = delegatedDriver.connect(delegatedUrl, info);
        
        LookupManager lookupManager = new LookupManager(encryptionService, connection);
        
        try (Statement stm = connection.createStatement()) {
            // TODO periodically reload table data if we assume database changes can happen without an application restart?
            List<String> tables = listTables(stm);
            
            lookupManager.initLookup(tables);
            
            return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), 
                    new Class[] {Connection.class}, 
                    new ConnectionInvocationHandler(connection, encryptionService, 
                            auditLogService, new SqlParser(tables), lookupManager));
        }
    }

    private List<String> listTables(Statement stm) throws SQLException {
        String database = stm.getConnection().getMetaData().getDatabaseProductName();
        List<String> result = new ArrayList<>();
        // if is more readable than switch
        DatabaseType databaseType = DatabaseType.findByName(database);
        if (databaseType == DatabaseType.MYSQL || databaseType == DatabaseType.MARIADB) {
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

}
