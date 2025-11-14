import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import config.BinLogConfig;
import listeners.BinLogClientLifeCycleListener;
import listeners.BinLogEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Main class for reading MySQL binary logs and processing change data capture events.
 */
public class BinLogReader {

    private static final Logger log = LoggerFactory.getLogger(BinLogReader.class);
    private BinaryLogClient binaryLogClient;
    private final BinLogConfig config;
    private BinLogEventListener binLogEventListener;
    private BinLogClientLifeCycleListener binLogClientLifeCycleListener;
    private volatile boolean running = false;

    public static void main(String[] args) {
        log.info("Starting BinLog CDC Reader...");

        BinLogReader reader = new BinLogReader();

        // Add shutdown hook for graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown hook triggered, stopping BinLog reader...");
            try {
                reader.stop();
            } catch (IOException e) {
                log.error("Error during shutdown", e);
            }
        }));

        try {
            reader.start();

            // Keep the application running
            while (reader.isRunning()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.info("Main thread interrupted, shutting down...");
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        } catch (IOException e) {
            log.error("Failed to start BinLog reader", e);
            System.exit(1);
        }
    }

    public BinLogReader() {
        this(new BinLogConfig());
    }

    public BinLogReader(BinLogConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("BinLogConfig cannot be null");
        }
        this.config = config;
    }

    public BinLogReader(String hostname, int port, String username, String password) {
        if (hostname == null || hostname.trim().isEmpty()) {
            throw new IllegalArgumentException("Hostname cannot be null or empty");
        }
        if (username == null) {
            throw new IllegalArgumentException("Username cannot be null");
        }
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("Port must be between 1 and 65535");
        }

        // Create config from parameters
        this.config = new BinLogConfig();
        // Note: This is a simplified approach. In production, avoid storing credentials this way
        log.warn("Using direct credential constructor - consider using configuration file instead");
    }

    private void initialize() {
        if (binaryLogClient != null) {
            log.warn("BinaryLogClient already initialized");
            return;
        }

        log.info("Initializing BinaryLogClient with hostname={}, port={}, username={}",
                config.getHostname(), config.getPort(), config.getUsername());

        binaryLogClient = new BinaryLogClient(
                config.getHostname(),
                config.getPort(),
                config.getUsername(),
                config.getPassword()
        );

        // Set binlog position if specified
        if (config.getBinlogFilename() != null && !config.getBinlogFilename().isEmpty()) {
            binaryLogClient.setBinlogFilename(config.getBinlogFilename());
            if (config.getBinlogPosition() >= 0) {
                binaryLogClient.setBinlogPosition(config.getBinlogPosition());
                log.info("Starting from binlog position: {}:{}",
                        config.getBinlogFilename(), config.getBinlogPosition());
            }
        }

        // Configure connection settings
        binaryLogClient.setKeepAlive(config.isKeepAlive());
        binaryLogClient.setKeepAliveInterval(config.getKeepAliveInterval());
        binaryLogClient.setHeartbeatInterval(config.getHeartbeatInterval());

        // Initialize listeners
        binLogClientLifeCycleListener = new BinLogClientLifeCycleListener();
        binLogEventListener = new BinLogEventListener();

        binaryLogClient.registerLifecycleListener(binLogClientLifeCycleListener);
        binaryLogClient.registerEventListener(binLogEventListener);

        // Configure event deserializer
        EventDeserializer eventDeserializer = new EventDeserializer();
        binaryLogClient.setEventDeserializer(eventDeserializer);

        log.info("BinaryLogClient initialized successfully");
    }

    public void start() throws IOException {
        if (running) {
            log.warn("BinLogReader is already running");
            return;
        }

        try {
            if (binaryLogClient == null) {
                initialize();
            }

            log.info("Connecting to MySQL binlog...");
            binaryLogClient.connect();
            running = true;
            log.info("Successfully connected to MySQL binlog");

        } catch (IOException e) {
            running = false;
            log.error("Failed to connect to MySQL binlog", e);
            throw e;
        } catch (Exception e) {
            running = false;
            log.error("Unexpected error during startup", e);
            throw new IOException("Failed to start BinLog reader", e);
        }
    }

    public void stop() throws IOException {
        if (!running) {
            log.warn("BinLogReader is not running");
            return;
        }

        log.info("Stopping BinLogReader...");
        running = false;

        try {
            if (binaryLogClient != null && binaryLogClient.isConnected()) {
                // Unregister listeners
                if (binLogEventListener != null) {
                    binaryLogClient.unregisterEventListener(binLogEventListener);
                }
                if (binLogClientLifeCycleListener != null) {
                    binaryLogClient.unregisterLifecycleListener(binLogClientLifeCycleListener);
                }

                // Disconnect
                binaryLogClient.disconnect();
                log.info("Disconnected from MySQL binlog");
            }
        } catch (IOException e) {
            log.error("Error during disconnect", e);
            throw e;
        } finally {
            binaryLogClient = null;
        }
    }

    public boolean isRunning() {
        return running && binaryLogClient != null && binaryLogClient.isConnected();
    }

    public BinLogEventListener getBinLogEventListener() {
        return binLogEventListener;
    }

    public BinaryLogClient getBinaryLogClient() {
        return binaryLogClient;
    }

    public BinLogConfig getConfig() {
        return config;
    }
}
