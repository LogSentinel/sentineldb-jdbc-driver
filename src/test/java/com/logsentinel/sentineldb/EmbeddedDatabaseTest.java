package com.logsentinel.sentineldb;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;

import org.h2.Driver;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import com.logsentinel.sentineldb.api.ExternalEncryptionApi;
import com.logsentinel.sentineldb.api.SearchSchemaApi;
import com.logsentinel.sentineldb.model.ExternalEncryptionResult;
import com.logsentinel.sentineldb.model.SearchSchema;
import com.logsentinel.sentineldb.model.SearchSchemaField;

public class EmbeddedDatabaseTest {

    private static final String H2_CONNECTION_STRING = "jdbc:h2:mem:public;INIT=create schema if not exists public;";
    private static final String CONNECTION_STRING = H2_CONNECTION_STRING.replace("jdbc:", "jdbc:sentineldb:") 
            + "sentineldbOrganizationId=x;sentineldbSecret=y;sentineldbDatastoreId=ab40b113-8538-4cd9-996e-c269ba1e9aa2";
    private static final String LOOKUP_KEY = "LOOKUP_KEY";

    @Test
    public void endToEndTest() throws Exception {
        DriverManager.registerDriver(new SentinelDBDriver());
        DriverManager.registerDriver(new Driver());
        
        SentinelDBClient mockClient = mock(SentinelDBClient.class);
        BiFunction<String, String, SentinelDBClient> builder = (orgId, secret) -> mockClient;
        ReflectionTestUtils.setField(ExternalEncryptionService.class, "clientBuilder", builder);
        ExternalEncryptionApi externalEncryptionApi = mock(ExternalEncryptionApi.class);
        SearchSchemaApi schemaApi = mock(SearchSchemaApi.class);
        when(mockClient.getExternalEncryptionActions()).thenReturn(externalEncryptionApi);
        when(mockClient.getSchemaActions()).thenReturn(schemaApi);
        when(externalEncryptionApi.encryptData(any(), anyString(), anyString(), anyString(), anyString())).thenAnswer(i -> createEncryptionResult(i.getArgument(4), i.getArgument(3)));
        when(externalEncryptionApi.decryptData(anyString(), any(), anyString(), anyString())).thenAnswer(i -> i.getArgument(0).toString());
        when(externalEncryptionApi.getLookupValue(any(), anyString())).thenReturn(LOOKUP_KEY);
        when(schemaApi.listSearchSchemas()).thenReturn(Collections.singletonList(createTestSchema()));
        
        try (Connection connection = DriverManager.getConnection(CONNECTION_STRING)) {
            // DDL to create the table with sensitive data
            try (Statement stm = connection.createStatement()) {
                stm.executeUpdate("CREATE TABLE sensitive (id INT auto_increment PRIMARY KEY, "
                        + "sensitive_field VARCHAR(100), searchable_sensitive_field VARCHAR(100), non_sensitive_field VARCHAR(100))");
            }
            
            // make another connection to get a fresh list of tables, as well as a raw H2 connection to check the raw, encrypted data
            try (Connection conn2 = DriverManager.getConnection(CONNECTION_STRING);
                    Connection connRaw = DriverManager.getConnection(H2_CONNECTION_STRING)) {
                
                // first insert some mix of encrypted and non-encrypted fields
                try (PreparedStatement pstm = conn2.prepareStatement("INSERT INTO sensitive(sensitive_field, searchable_sensitive_field, non_sensitive_field) VALuES (?, ?, ?)")) {
                    pstm.setString(1, "sensitive");
                    pstm.setString(2, "sensitive_searchable");
                    pstm.setString(3, "non_sensitive");
                    pstm.executeUpdate();
                }
                testCurrentData(conn2, connRaw, "sensitive", "sensitive_searchable", "non_sensitive");
                
                // then test an UPDATE query by ID 
                try (PreparedStatement pstm = conn2.prepareStatement("UPDATE sensitive SET searchable_sensitive_field=?, sensitive_field=? WHERE id=?")) {
                    pstm.setString(1, "sensitive_searchable2");
                    pstm.setString(2, "sensitive2");
                    pstm.setInt(3, 1);
                    
                    pstm.executeUpdate();
                }
                testCurrentData(conn2, connRaw, "sensitive2", "sensitive_searchable2", "non_sensitive");
                
                // then test an UPDATE with non-prepared statement 
                try (Statement stm = conn2.createStatement()) {
                    stm.executeUpdate("UPDATE sensitive SET searchable_sensitive_field='sensitive_searchable3', sensitive_field='sensitive3', non_sensitive_field='non_sensitive2' WHERE id=1");
                }
                
                testCurrentData(conn2, connRaw, "sensitive3", "sensitive_searchable3", "non_sensitive2");
                
                // finally test a DELETE 
                try (Statement stm = conn2.createStatement()) {
                    stm.executeUpdate("DELETE FROM sensitive WHERE id=1");
                    ResultSet rs = stm.executeQuery("SELECT * FROM sensitive");
                    assertThat(rs.next(), equalTo(false));
                }
                
                // also test an out-of-order insert (columns ordered differently than in the schema definition)
                try (PreparedStatement pstm = conn2.prepareStatement("INSERT INTO sensitive(searchable_sensitive_field, sensitive_field, non_sensitive_field) VALuES (?, ?, ?)")) {
                    pstm.setString(1, "sensitive_searchable");
                    pstm.setString(2, "sensitive");
                    pstm.setString(3, "non_sensitive");
                    pstm.executeUpdate();
                }
                testCurrentData(conn2, connRaw, "sensitive", "sensitive_searchable", "non_sensitive");
            }
        }
    }

    public void testCurrentData(Connection conn2, Connection connRaw, String... args) throws SQLException {
        // then select them back to see if they will be received unencrypted
        try (Statement stm = conn2.createStatement()) {
            ResultSet rs = stm.executeQuery("SELECT * FROM sensitive");
            rs.next();
            assertThat(rs.getString(2), equalTo(args[0]));
            assertThat(rs.getString(3), equalTo(args[1]));
            assertThat(rs.getString(4), equalTo(args[2]));
            
        }

        // then select them without the proxy to see if they are stored encrypted
        try (Statement stm = connRaw.createStatement()) {
            ResultSet rs = stm.executeQuery("SELECT * FROM sensitive");
            rs.next();
            assertThat(rs.getString(2), endsWith(Base64.getEncoder().encodeToString(args[0].getBytes())));
            assertThat(rs.getString(3), endsWith(Base64.getEncoder().encodeToString(args[1].getBytes())));
            assertThat(rs.getString(4), equalTo(args[2]));
            assertThat(rs.getString(5), equalTo(LOOKUP_KEY));
        }
        
        // then check the WHERE clause
        try (PreparedStatement pstm = conn2.prepareStatement("SELECT * FROM sensitive WHERE searchable_sensitive_field=?")) {
            pstm.setString(1, "sensitive_searchable");
            ResultSet rs = pstm.executeQuery();
            rs.next();
            assertThat(rs.getString(2), equalTo(args[0]));
            assertThat(rs.getString(3), equalTo(args[1]));
            assertThat(rs.getString(4), equalTo(args[2]));
        }
        
        // then check the WHERE clause without prepared sdtatement
        try (Statement stm = conn2.createStatement()) {
            ResultSet rs = stm.executeQuery("SELECT * FROM sensitive WHERE searchable_sensitive_field='sensitive_searchable'");
            rs.next();
            assertThat(rs.getString(2), equalTo(args[0]));
            assertThat(rs.getString(3), equalTo(args[1]));
            assertThat(rs.getString(4), equalTo(args[2]));
        }
    }

    private ExternalEncryptionResult createEncryptionResult(Object plaintext, Object fieldName) {
        ExternalEncryptionResult result = new ExternalEncryptionResult();
        result.setCiphertext(Base64.getEncoder().encodeToString(plaintext.toString().getBytes()));
        if (fieldName.equals("searchable_sensitive_field")) {
            result.setLookupKeys(Arrays.asList(LOOKUP_KEY));
        } else {
            result.setLookupKeys(Collections.emptyList());
        }
        return result;
    }

    private SearchSchema createTestSchema() {
        SearchSchema schema = new SearchSchema();
        schema.setRecordType("sensitive");
        List<SearchSchemaField> fields = new ArrayList<>();
        SearchSchemaField field1 = new SearchSchemaField();
        field1.setIndexed(false);
        field1.setAnalyzed(false);
        field1.setName("sensitive_field");
        fields.add(field1);
        SearchSchemaField field2 = new SearchSchemaField();
        field2.setIndexed(true);
        field2.setAnalyzed(false);
        field2.setName("searchable_sensitive_field");
        fields.add(field2);
        
        schema.setFields(fields);
        return schema;
    }
}
