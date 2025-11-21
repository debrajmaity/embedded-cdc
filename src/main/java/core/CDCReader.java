package core;

import events.Event;
import events.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Unified CDC Reader that supports multiple database types.
 * Automatically detects the database type from configuration and creates the appropriate client.
 */
public class CDCReader {

    private static final Logger logger = LoggerFactory.getLogger(CDCReader.class);
    private static final String DEFAULT_CONFIG = "cdc.properties";

    private CDCClient client;
    private String databaseType;
    private volatile boolean isRunning = false;

    /**
     * Create a CDC reader from the default configuration file.
     */
    public CDCReader() throws Exception {
        this(DEFAULT_CONFIG);
    }

    /**
     * Create a CDC reader from a configuration file.
     * @param configFile path to the configuration file
     */
    public CDCReader(String configFile) throws Exception {
        Properties props = loadProperties(configFile);
        this.databaseType = props.getProperty("database.type", "mysql").toLowerCase();
        this.client = createClient(databaseType, configFile);
    }

    /**
     * Create a CDC reader with a specific database type and configuration file.
     * @param databaseType the database type (mysql, postgresql)
     * @param configFile path to the configuration file
     */
    public CDCReader(String databaseType, String configFile) throws Exception {
        this.databaseType = databaseType.toLowerCase();
        this.client = createClient(this.databaseType, configFile);
    }

    /**
     * Create a CDC reader with an existing client.
     * @param client the CDC client to use
     */
    public CDCReader(CDCClient client) {
        this.client = client;
        this.databaseType = client.getDatabaseType();
    }

    private Properties loadProperties(String configFile) throws IOException {
        Properties props = new Properties();
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(configFile)) {
            if (is != null) {
                props.load(is);
            }
        }
        return props;
    }

    private CDCClient createClient(String type, String configFile) throws Exception {
        return switch (type) {
            case "postgresql", "postgres", "pg" -> {
                postgresql.PostgreSQLConfig config = configFile != null
                    ? new postgresql.PostgreSQLConfig(configFile)
                    : new postgresql.PostgreSQLConfig();
                yield new postgresql.PostgreSQLCDCClient(config);
            }
            case "mysql" -> {
                // For backward compatibility, create a MySQL client wrapper
                yield new MySQLClientAdapter(configFile);
            }
            default -> throw new IllegalArgumentException("Unsupported database type: " + type);
        };
    }

    /**
     * Start capturing CDC events.
     */
    public void start() throws Exception {
        logger.info("Starting CDC Reader for {}", databaseType);

        client.registerEventHandler(this::processEvent);

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                stop();
            } catch (Exception e) {
                logger.error("Error during shutdown", e);
            }
        }));

        client.connect();
        isRunning = true;

        logger.info("CDC Reader started successfully for {}", databaseType);
    }

    /**
     * Stop capturing CDC events.
     */
    public void stop() throws Exception {
        if (client != null && isRunning) {
            logger.info("Stopping CDC Reader");
            client.disconnect();
            isRunning = false;
            logger.info("CDC Reader stopped");
        }
    }

    /**
     * Check if the reader is running.
     */
    public boolean isRunning() {
        return isRunning && client != null && client.isConnected();
    }

    /**
     * Process a CDC event. Override to implement custom handling.
     */
    protected void processEvent(Event event, EventType eventType) {
        logger.info("Event: {} on {}.{}", eventType.getValue(), event.getDatabase(), event.getTable());
        logger.debug("  Before: {}", event.getChangesBefore());
        logger.debug("  After: {}", event.getChangesAfter());
    }

    /**
     * Register an additional event handler.
     */
    public void registerEventHandler(CDCClient.CDCEventHandler handler) {
        client.registerEventHandler(handler);
    }

    /**
     * Get the underlying client.
     */
    public CDCClient getClient() {
        return client;
    }

    /**
     * Get the database type.
     */
    public String getDatabaseType() {
        return databaseType;
    }

    /**
     * Main method for standalone execution.
     */
    public static void main(String[] args) {
        try {
            CDCReader reader;

            if (args.length >= 2) {
                reader = new CDCReader(args[0], args[1]);
            } else if (args.length == 1) {
                reader = new CDCReader(args[0]);
            } else {
                reader = new CDCReader();
            }

            reader.start();

            // Keep running
            while (reader.isRunning()) {
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            logger.error("Error starting CDC Reader", e);
            System.exit(1);
        }
    }

    /**
     * Adapter for the existing MySQL BinaryLogClient.
     */
    private static class MySQLClientAdapter implements CDCClient {
        private final config.BinLogConfig config;
        private com.github.shyiko.mysql.binlog.BinaryLogClient binaryLogClient;
        private final java.util.List<CDCEventHandler> handlers = new java.util.concurrent.CopyOnWriteArrayList<>();
        private volatile boolean connected = false;

        public MySQLClientAdapter(String configFile) throws IOException {
            this.config = configFile != null ? new config.BinLogConfig(configFile) : new config.BinLogConfig();
        }

        @Override
        public void connect() throws Exception {
            binaryLogClient = new com.github.shyiko.mysql.binlog.BinaryLogClient(
                config.getHostname(),
                config.getPort(),
                config.getUsername(),
                config.getPassword()
            );

            String binlogFilename = config.getBinlogFilename();
            if (binlogFilename != null && !binlogFilename.isEmpty()) {
                binaryLogClient.setBinlogFilename(binlogFilename);
            }

            long binlogPosition = config.getBinlogPosition();
            if (binlogPosition > 0) {
                binaryLogClient.setBinlogPosition(binlogPosition);
            }

            binaryLogClient.setKeepAlive(config.isKeepAlive());
            binaryLogClient.setKeepAliveInterval(config.getKeepAliveInterval());
            binaryLogClient.setHeartbeatInterval(config.getHeartbeatInterval());

            // Use existing event listener with adapter
            listeners.BinLogEventListener eventListener = new listeners.BinLogEventListener() {
                @Override
                protected void processEvent(Event event, events.EventType eventType) {
                    for (CDCEventHandler handler : handlers) {
                        handler.onEvent(event, eventType);
                    }
                }
            };

            binaryLogClient.registerEventListener(eventListener);
            binaryLogClient.registerLifecycleListener(new listeners.BinLogClientLifeCycleListener());

            binaryLogClient.connect();
            connected = true;
        }

        @Override
        public void disconnect() throws Exception {
            if (binaryLogClient != null) {
                binaryLogClient.disconnect();
            }
            connected = false;
        }

        @Override
        public boolean isConnected() {
            return connected && binaryLogClient != null && binaryLogClient.isConnected();
        }

        @Override
        public void registerEventHandler(CDCEventHandler handler) {
            handlers.add(handler);
        }

        @Override
        public String getDatabaseType() {
            return "mysql";
        }
    }
}
