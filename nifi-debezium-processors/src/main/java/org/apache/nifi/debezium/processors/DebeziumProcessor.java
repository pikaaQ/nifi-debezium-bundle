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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
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
                DATABASE_PASSWORD, DATABASE_DBNAME, DATABASE_SERVER_NAME, EXT_JSON
        );

        relationships = new HashSet<>(Arrays.asList(
                REL_SUCCESS
        ));
    }

    private Thread debeziumEngineThread;
    private DebeziumEngine<ChangeEvent<String, String>> debeziumEngine;
    private volatile ProcessSession currentSession;


    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        ComponentLog log = this.getLogger();
        StateManager stateManager = context.getStateManager();

        if (currentSession == null) {
            currentSession = sessionFactory.createSession();
        }
        // Create a debeziumEngine if we don't have one
        if (debeziumEngine == null) {
            Properties properties = new Properties();
            properties.putAll(context.getAllProperties());

            String extJson = properties.remove(EXT_JSON.getName()).toString();
            try {
                properties.putAll(new ObjectMapper().readValue(extJson, HashMap.class));
            } catch (IOException e) {
                e.printStackTrace();
            }

            logger.info("debezium engine create: {}", properties.toString());


            debeziumEngine = DebeziumEngine.create(CloudEvents.class)
                    .using(properties)
                    .using(new DebeziumEngine.ConnectorCallback() {
                        @Override
                        public void connectorStarted() {

                        }

                        @Override
                        public void connectorStopped() {
                            try {
                                debeziumEngine.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }).using((success, message, error) -> {

                    })
                    .notifying(record -> {
                        String value = record.value();
                        logger.info("notifying: "+value);
                        FlowFile flowFile = currentSession.create();
                        flowFile = currentSession.write(flowFile, outputStream -> {
                            outputStream.write(value.getBytes());
                            outputStream.flush();
                        });
                        currentSession.transfer(flowFile, REL_SUCCESS);
                        currentSession.getProvenanceReporter().receive(flowFile, "<unknown>");
                        currentSession.commit();
                    }).build();

            
        }

        // If the debeziumEngine has been stop, try to restart
        if (debeziumEngineThread.isAlive()) {
            try {
                debeziumEngine.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            debeziumEngine = null;
            // Try again later
            context.yield();
            return;
        }
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
