package org.apache.nifi.debezium.processors;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.CloudEvents;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.debezium.offset.storage.StaticMapOffsetStore;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A processor to retrieve Change Data Capture (CDC) events and send them as flow files.
 */
@TriggerSerially
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"CDC", "Debezium", "MySQL", "PostgreSQL", "Oracle", "SQL Server", "Db2", "Cassandra", "MongoDB"})
@CapabilityDescription("Debezium is a set of distributed services to capture changes in your databases so that your applications can see those changes and respond to them. " +
        "Debezium records all row-level changes within each database table in a change event stream, " +
        "and applications simply read these streams to see the change events in the same order in which they occurred.")
@Stateful(scopes = Scope.CLUSTER, description = "Information such as a 'pointer' to the current CDC event in the database is stored by this processor, " +
        "such that it can continue from the same location if restarted.")
public class DebeziumProcessor extends AbstractSessionFactoryProcessor {
    private final static Logger logger = LoggerFactory.getLogger(DebeziumProcessor.class);
    private static List<PropertyDescriptor> propDescriptors;
    private static Set<Relationship> relationships;

    public static final PropertyDescriptor NAME = new PropertyDescriptor.Builder()
            .name("name")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor CONNECTOR_CLASS = new PropertyDescriptor.Builder()
            .name("connector.class")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor DATABASE_HOSTNAME = new PropertyDescriptor.Builder()
            .name("database.hostname")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor DATABASE_PORT = new PropertyDescriptor.Builder()
            .name("database.port")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();
    public static final PropertyDescriptor DATABASE_USER = new PropertyDescriptor.Builder()
            .name("database.user")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor DATABASE_PASSWORD = new PropertyDescriptor.Builder()
            .name("database.password")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor DATABASE_DBNAME = new PropertyDescriptor.Builder()
            .name("database.dbname")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor DATABASE_SERVER_NAME = new PropertyDescriptor.Builder()
            .name("database.server.name")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor OFFSET_POINTER_TOPIC = new PropertyDescriptor.Builder()
            .name("offset.storage.topic")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor EXT_JSON = new PropertyDescriptor.Builder()
            .name("ext.json")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully created FlowFile from Debezium engine.")
            .build();

    static {
        propDescriptors = Arrays.asList(
                NAME, CONNECTOR_CLASS, DATABASE_HOSTNAME, DATABASE_PORT, DATABASE_USER,
                DATABASE_PASSWORD, DATABASE_DBNAME, DATABASE_SERVER_NAME, OFFSET_POINTER_TOPIC, EXT_JSON
        );

        relationships = new HashSet<>(Arrays.asList(
                REL_SUCCESS
        ));
    }

    private ExecutorService executor  = Executors.newSingleThreadExecutor();
    private DebeziumEngine<ChangeEvent<String, String>> debeziumEngine;
    private volatile ProcessSession currentSession;
    private String topic;

    public void setup(ProcessContext context) throws IOException {

        Properties properties = new Properties();
        properties.putAll(context.getAllProperties());

        String extJson = properties.remove(EXT_JSON.getName()).toString();
        try {
            properties.putAll(new ObjectMapper().readValue(extJson, HashMap.class));
        } catch (IOException e) {
            logger.error("extension properties not a legal json map: " + e.getMessage());
        }

        properties.setProperty("offset.storage", "org.apache.nifi.debezium.offset.storage.StaticMapOffsetStore");

        logger.info("debezium engine create: {}", properties.toString());

        debeziumEngine = DebeziumEngine.create(CloudEvents.class)
                        .using(properties)
                        .using(new DebeziumEngine.ConnectorCallback() {
                            @Override
                            public void connectorStopped() {
                                logger.warn("debezium engine connector stopped.");
                                stop(context);
                            }
                        })
                        .using((success, message, error) -> {
                            if(error != null){
                                logger.warn("debezium engine stop by error: " + error.getMessage());
                            }
                        })
                        .notifying((records, committer) -> {
                            try{
                                for(ChangeEvent<String, String> record : records){
                                    sendOneRecord(record);
                                    committer.markProcessed(record);
                                }
                                committer.markBatchFinished();
                                currentSession.commit();
                            }catch (Exception e){
                                currentSession.rollback();
                            }
                        }).build();
    }

    private void sendOneRecord(ChangeEvent<String, String> record){
        Object value = record.value();
        if(value != null){
            logger.info("notifying: "+ record);
            FlowFile flowFile = currentSession.create();
            flowFile = currentSession.write(flowFile, outputStream -> {
                outputStream.write(value.toString().getBytes());
                outputStream.flush();
            });
            currentSession.transfer(flowFile, REL_SUCCESS);
            currentSession.getProvenanceReporter().receive(flowFile, "<unknown>");
        }
    }

    private void run(ProcessSessionFactory sessionFactory) {
        if (currentSession == null) {
            currentSession = sessionFactory.createSession();
        }

        executor.execute(debeziumEngine);
    }

    private void stop(ProcessContext context) {
        if(debeziumEngine != null){
            try {
                debeziumEngine.close();
                saveOffset(context);
                debeziumEngine = null;
            } catch (IOException e) {
                logger.error("fail to stop debeziumEngine: ", e);
                throw new ProcessException(e);
            }
        }

    }

    // Synchronize the offset. See StaticMapOffsetStore, I use a static ConcurrentHashMap to store the offset.
    private void setOffset(ProcessContext context) throws IOException {
        topic = context.getProperty("offset.storage.topic").getValue();
        StateManager stateManager = context.getStateManager();
        StateMap curStateMap = stateManager.getState(Scope.CLUSTER);
        StaticMapOffsetStore.setTopicOffset(topic, curStateMap.toMap());
    }

    private void saveOffset(ProcessContext context) throws IOException {
        topic = context.getProperty("offset.storage.topic").getValue();
        StateManager stateManager = context.getStateManager();
        stateManager.setState(StaticMapOffsetStore.getTopicOffset(topic), Scope.CLUSTER);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        ComponentLog log = this.getLogger();

        // Create a debeziumEngine if we don't have one
        if (debeziumEngine == null) {
            try {
                setup(context);
            } catch (IOException e) {
                log.error("fail to setup debezium: " + e.getMessage());
                logger.error("fail to setup debezium: ", e);
                context.yield();
                return;
            }

            try {
                setOffset(context);
            } catch (IOException e) {
                log.error("fail to setup offset: " + e.getMessage());
                logger.error("fail to setup offset: ", e);
                context.yield();
                return;
            }

            run(sessionFactory);
        }

        try {
            saveOffset(context);
        } catch (IOException e) {
            log.error("fail to flush offset: ", e);
            logger.error("fail to flush offset: ", e);
        }
    }

    @OnStopped
    public void onStopped(ProcessContext context) {
        stop(context);
    }

    @OnShutdown
    public void onShutdown(ProcessContext context) {
        stop(context);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propDescriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }
}
