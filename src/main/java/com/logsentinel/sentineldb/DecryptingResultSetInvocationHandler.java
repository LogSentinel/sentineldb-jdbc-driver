package com.logsentinel.sentineldb;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.sql.ResultSet;

public class DecryptingResultSetInvocationHandler implements InvocationHandler {

    private ResultSet resultSet;
    private ExternalEncryptionService encryptionService;
    
    public DecryptingResultSetInvocationHandler(ResultSet resultSet, ExternalEncryptionService encryptionService) {
        this.resultSet = resultSet;
        this.encryptionService = encryptionService;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (method.getName().equals("getString")) {
            String value = (String) method.invoke(resultSet, args);
            
            if (encryptionService.isEncrypted(value)) {
                return encryptionService.decryptString(value);
            }
            return value;
            // TODO getBytes / getClob /getCharacterStream / getBlob
        } else {
            return method.invoke(resultSet, args);
        }
    }

}
