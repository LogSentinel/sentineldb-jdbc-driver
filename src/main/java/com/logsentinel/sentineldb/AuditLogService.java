package com.logsentinel.sentineldb;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.logsentinel.LogSentinelClient;
import com.logsentinel.LogSentinelClientBuilder;
import com.logsentinel.model.ActionData;
import com.logsentinel.model.ActorData;

public class AuditLogService {

    private String organizationId;
    private String secret;
    private String applicationId;
    private String url;
    private Method actorDetailsMethod;
    private LogSentinelClient client;

    private ExecutorService executor = Executors.newFixedThreadPool(5);

    public AuditLogService(String organizationId, String secret, String applicationId, String url,
            Method actorDetailsMethod) {
        this.organizationId = organizationId;
        this.secret = secret;
        this.applicationId = applicationId;
        this.url = url;
        this.actorDetailsMethod = actorDetailsMethod;
    }

    public void init() {
        if (organizationId != null) {
            client = LogSentinelClientBuilder.create(applicationId, organizationId, secret).setBasePath(url).build();
        } else {
            System.out.println("Not using secure logging due to missing configuration properties trailsOrganizationId, trailsSecret and trailsApplicationId");
        }
    }

    public void logQuery(String query, List<String> columnsReturned) {
        if (client == null) {
            return;
        }
        executor.submit(() -> {
            try {
                ActorData actorData = getActorData();
                client.getAuditLogActions().log(actorData,
                        new ActionData<>().details(new QueryDetails(query, columnsReturned)));
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        });
    }

    public void logQuery(String query) {
        if (client == null) {
            return;
        }
        executor.submit(() -> {
            try {
                ActorData actorData = getActorData();
                client.getAuditLogActions().log(actorData, new ActionData<>().details(new QueryDetails(query, null)));
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        });
    }

    @SuppressWarnings("unchecked")
    public ActorData getActorData() {
        ActorData actorData = new ActorData();
        if (actorDetailsMethod != null) {
            try {
                Optional<String[]> actorDetails = (Optional<String[]>) actorDetailsMethod.invoke(null);
                actorDetails.ifPresent(s -> {
                    actorData.setActorId(s[0]);
                    if (s.length > 1) {
                        actorData.setActorDisplayName(s[1]);
                    }
                });
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                e.printStackTrace();
            }
        }
        return actorData;
    }

    public static class QueryDetails {
        private String query;
        private List<String> columnNames;

        public QueryDetails(String query, List<String> columnNames) {
            this.query = query;
            this.columnNames = columnNames;
        }

        public List<String> getColumnNames() {
            return columnNames;
        }

        public void setColumnNames(List<String> columnNames) {
            this.columnNames = columnNames;
        }

        public String getQuery() {
            return query;
        }

        public void setQuery(String query) {
            this.query = query;
        }
    }

}
