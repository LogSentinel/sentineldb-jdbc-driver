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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
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
        when(externalEncryptionApi.decryptData(anyString(), any(), anyString(), anyString())).thenAnswer(i -> i.getArgument(0).toString().replace("_ENCRYPTED", ""));
        when(schemaApi.listSearchSchemas()).thenReturn(Collections.singletonList(createTestSchema()));
        
        try (Connection connection = DriverManager.getConnection(CONNECTION_STRING)) {
            // DDL to create the table with sensitive data
            try (Statement stm = connection.createStatement()) {
                stm.executeUpdate("CREATE TABLE sensitive (id int auto_increment NOT NULL, "
                        + "sensitive_field VARCHAR(100), searchable_sensitive_field VARCHAR(100), non_sensitive_field VARCHAR(100))");
            }
            
            // make another connection to get a fresh list of tables, as well as a raw H2 connection to check the raw, encrypted data
            try (Connection conn2 = DriverManager.getConnection(CONNECTION_STRING);
                    Connection connRaw = DriverManager.getConnection(H2_CONNECTION_STRING)) {
                
                // first insert some mix of encrypted and non-encrypted fields
                try (PreparedStatement pstm = conn2.prepareStatement("INSERT INTO sensitive(sensitive_field, searchable_sensitive_field, non_sensitive_field) VALUES (?, ?, ?)")) {
                    pstm.setString(1, "sensitive");
                    pstm.setString(2, "sensitive_searchable");
                    pstm.setString(3, "non_sensitive");
                    pstm.executeUpdate();
                }
                
                // then select them back to see if they will be received unencrypted
                try (Statement stm = conn2.createStatement()) {
                    ResultSet rs = stm.executeQuery("SELECT * FROM sensitive");
                    rs.next();
                    assertThat(rs.getString(2), equalTo("sensitive"));
                    assertThat(rs.getString(3), equalTo("sensitive_searchable"));
                    assertThat(rs.getString(4), equalTo("non_sensitive"));
                }
                
             // then select them back to see if they will be received unencrypted
                try (Statement stm = connRaw.createStatement()) {
                    ResultSet rs = stm.executeQuery("SELECT * FROM sensitive");
                    rs.next();
                    assertThat(rs.getString(2), endsWith("sensitive_ENCRYPTED"));
                    assertThat(rs.getString(3), endsWith("sensitive_searchable_ENCRYPTED"));
                    assertThat(rs.getString(4), equalTo("non_sensitive"));
                    assertThat(rs.getString(5), equalTo(LOOKUP_KEY));
                }
            }
        }
        
            
            // insert
            // select
            // update
            // select
            // delete
            // select
    }

    private ExternalEncryptionResult createEncryptionResult(Object plaintext, Object fieldName) {
        ExternalEncryptionResult result = new ExternalEncryptionResult();
        result.setCiphertext(plaintext + "_ENCRYPTED");
        // TODO
        if (fieldName.equals("searchable_sensitive_field")) {
            result.setLookupKeys(Arrays.asList(LOOKUP_KEY));
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
