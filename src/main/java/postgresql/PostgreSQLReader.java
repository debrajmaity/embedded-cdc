package postgresql;

import core.CDCClient;
import events.Event;
import events.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main entry point for PostgreSQL CDC.
 * Provides a simple API to start capturing change events from PostgreSQL.
 */
public class PostgreSQLReader {

    private static final Logger logger = LoggerFactory.getLogger(PostgreSQLReader.class);

    private final PostgreSQLConfig config;
    private PostgreSQLCDCClient client;
    private volatile boolean isRunning = false;

    /**
     * Create a PostgreSQL reader with default configuration.
     */
    public PostgreSQLReader() {
        this.config = new PostgreSQLConfig();
    }

    /**
     * Create a PostgreSQL reader with the specified configuration.
     * @param config PostgreSQL configuration
     */
    public PostgreSQLReader(PostgreSQLConfig config) {
        this.config = config;
    }

    /**
     * Create a PostgreSQL reader from a configuration file.
     * @param configFile path to the configuration file
     */
    public PostgreSQLReader(String configFile) throws Exception {
        this.config = new PostgreSQLConfig(configFile);
    }

    /**
     * Start capturing CDC events.
     */
    public void start() throws Exception {
        logger.info("Starting PostgreSQL CDC Reader");

        client = new PostgreSQLCDCClient(config);
        client.registerEventHandler(this::processEvent);

        // Add shutdown hook for graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                stop();
            } catch (Exception e) {
                logger.error("Error during shutdown", e);
            }
        }));

        client.connect();
        isRunning = true;

        logger.info("PostgreSQL CDC Reader started successfully");
    }

    /**
     * Stop capturing CDC events.
     */
    public void stop() throws Exception {
        if (client != null && isRunning) {
            logger.info("Stopping PostgreSQL CDC Reader");
            client.disconnect();
            isRunning = false;
            logger.info("PostgreSQL CDC Reader stopped");
        }
    }

    /**
     * Check if the reader is currently running.
     * @return true if running, false otherwise
     */
    public boolean isRunning() {
        return isRunning && client != null && client.isConnected();
    }

    /**
     * Process a CDC event. Override this method to implement custom event handling.
     * @param event the captured event
     * @param eventType the type of event (INSERT, UPDATE, DELETE)
     */
    protected void processEvent(Event event, EventType eventType) {
        logger.info("Event: {} on {}.{}", eventType.getValue(), event.getDatabase(), event.getTable());
        logger.debug("  Before: {}", event.getChangesBefore());
        logger.debug("  After: {}", event.getChangesAfter());
    }

    /**
     * Get the underlying CDC client.
     * @return the PostgreSQL CDC client
     */
    public PostgreSQLCDCClient getClient() {
        return client;
    }

    /**
     * Get the configuration.
     * @return the PostgreSQL configuration
     */
    public PostgreSQLConfig getConfig() {
        return config;
    }

    /**
     * Register an additional event handler.
     * @param handler the event handler to register
     */
    public void registerEventHandler(CDCClient.CDCEventHandler handler) {
        if (client != null) {
            client.registerEventHandler(handler);
        }
    }

    /**
     * Main method for standalone execution.
     */
    public static void main(String[] args) {
        try {
            PostgreSQLReader reader;

            if (args.length > 0) {
                reader = new PostgreSQLReader(args[0]);
            } else {
                reader = new PostgreSQLReader();
            }

            reader.start();

            // Keep the application running
            while (reader.isRunning()) {
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            logger.error("Error starting PostgreSQL CDC Reader", e);
            System.exit(1);
        }
    }
}
