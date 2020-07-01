package com.logsentinel.sentineldb;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;

import com.logsentinel.sentineldb.model.ExternalEncryptionResult;
import com.logsentinel.sentineldb.model.SearchSchema;
import com.logsentinel.sentineldb.model.SearchSchemaField;

public class ExternalEncryptionService {
    
    private static final String ENCRYPTED_FIELD_PREFIX = "sdbenc:";
    
    private String organizationId;
    private String secret;
    private UUID datastoreId;
    
    private SentinelDBClient sentinelDBClient;

    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    
    private Map<String, SearchSchema> cachedSchemas = new HashMap<>();
    private Map<String, Boolean> shouldIndexMap = new HashMap<>();
    private Map<String, List<String>> indexedColumns = new HashMap<>();
    
    public ExternalEncryptionService(String organizationId, String secret, UUID datastoreId) {
        this.organizationId = organizationId;
        this.secret = secret;
        this.datastoreId = datastoreId;
    }

    public void init() {
        sentinelDBClient = SentinelDBClientBuilder.create(organizationId, secret).build();
        scheduler.scheduleAtFixedRate(() -> {
            cachedSchemas = sentinelDBClient.getSchemaActions().listSearchSchemas()
                    .stream().filter(s -> !s.getFields().isEmpty())
                    .collect(Collectors.toMap(s -> s.getRecordType(), Function.identity()));
            
            shouldIndexMap.clear();
            for (SearchSchema schema : cachedSchemas.values()) {
                for (SearchSchemaField field : schema.getFields()) {
                    shouldIndexMap.put(schema.getRecordType() + ":" + field.getName(), field.isIndexed());
                }
                List<String> indexedNotAnalyzedFields = schema.getFields()
                        .stream().filter(f -> f.isIndexed() && !f.isAnalyzed()).map(SearchSchemaField::getName)
                        .collect(Collectors.toList());
                if (!indexedNotAnalyzedFields.isEmpty()) {
                    indexedColumns.put(schema.getRecordType(), indexedNotAnalyzedFields);
                }
                
            }
        }, 0, 10, TimeUnit.MINUTES);
    }
    
    public Pair<String, List<String>> encryptString(String plaintext, String tableName, String columnName, Object id) {
        plaintext = extendPlaintext(plaintext);
        ExternalEncryptionResult result = sentinelDBClient.getExternalEncryptionActions().encryptData(datastoreId, String.valueOf(id), tableName, columnName, plaintext);
        return Pair.of(ENCRYPTED_FIELD_PREFIX + tableName + ":" + id + ":" + result.getCiphertext(), result.getLookupKeys());
    }
    
    private String extendPlaintext(String plaintext) {
        // TODO extend the plaintext with either salt or a configurable/generated-once value, in case it's to small, which would risk its privacy
        return plaintext;
    }

    public boolean isEncrypted(String enrichedCiphertext) {
        return enrichedCiphertext.startsWith(ENCRYPTED_FIELD_PREFIX);
    }
    
    public String decryptString(String enrichedCiphertext) {
        String[] ciphertextElements = enrichedCiphertext.split(":");
        String tableName = ciphertextElements[1];
        String id = ciphertextElements[2];
        String ciphertext = ciphertextElements[3];
        
        return sentinelDBClient.getExternalEncryptionActions().decryptData(ciphertext, datastoreId, id, tableName);
    }
    
    public String getLookupKey(String plaintext) {
        return sentinelDBClient.getExternalEncryptionActions().getLookupValue(datastoreId, plaintext);
    }
    
    public List<String> getSearchableEncryptedColumns(String table) {
        return indexedColumns.get(table);
    }
    
    public boolean tableConstainsSensitiveData(String table) {
        return cachedSchemas.containsKey(table);
    }
    
    public boolean isEncrypted(String table, String columnName) {
        return shouldIndexMap.containsKey(table + ":" + columnName);
    }
    
    public boolean isSearchable(String table, String columnName) {
        return isEncrypted(table, columnName) && shouldIndexMap.get(table + ":" + columnName);
    }
    
}
