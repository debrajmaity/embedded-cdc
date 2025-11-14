package listeners;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lifecycle listener for BinaryLogClient to handle connection events and failures.
 */
public class BinLogClientLifeCycleListener implements BinaryLogClient.LifecycleListener {
    private static final Logger log = LoggerFactory.getLogger(BinLogClientLifeCycleListener.class);

    private volatile boolean connected = false;
    private long connectionTimestamp = -1;
    private int communicationFailureCount = 0;
    private int deserializationFailureCount = 0;

    @Override
    public void onConnect(BinaryLogClient binaryLogClient) {
        connected = true;
        connectionTimestamp = System.currentTimeMillis();
        communicationFailureCount = 0;
        deserializationFailureCount = 0;

        log.info("Successfully connected to MySQL binlog - {}:{}, binlog file: {}, position: {}",
                binaryLogClient.getBinlogFilename() != null ? binaryLogClient.getBinlogFilename() : "current",
                binaryLogClient.getBinlogPosition());
    }

    @Override
    public void onCommunicationFailure(BinaryLogClient binaryLogClient, Exception e) {
        communicationFailureCount++;
        long uptimeMs = connectionTimestamp > 0 ? System.currentTimeMillis() - connectionTimestamp : 0;

        log.error("Communication failure #{} after {}ms of uptime - Connection: {}:{}, Error: {}",
                communicationFailureCount,
                uptimeMs,
                binaryLogClient.getBinlogFilename(),
                binaryLogClient.getBinlogPosition(),
                e.getMessage(),
                e);

        // Log additional context for debugging
        if (communicationFailureCount > 10) {
            log.error("CRITICAL: Communication failures exceeded 10, please check network connectivity and MySQL server status");
        }
    }

    @Override
    public void onEventDeserializationFailure(BinaryLogClient binaryLogClient, Exception e) {
        deserializationFailureCount++;

        log.error("Event deserialization failure #{} - Binlog position: {}:{}, Error: {}",
                deserializationFailureCount,
                binaryLogClient.getBinlogFilename(),
                binaryLogClient.getBinlogPosition(),
                e.getMessage(),
                e);

        // Log warning for repeated failures
        if (deserializationFailureCount > 5) {
            log.warn("Multiple deserialization failures detected. This may indicate incompatible binlog format or corrupted events.");
        }
    }

    @Override
    public void onDisconnect(BinaryLogClient binaryLogClient) {
        boolean wasConnected = connected;
        connected = false;

        long uptimeMs = connectionTimestamp > 0 ? System.currentTimeMillis() - connectionTimestamp : 0;

        if (wasConnected) {
            log.info("Disconnected from MySQL binlog after {}ms - Last position: {}:{}, Communication failures: {}, Deserialization failures: {}",
                    uptimeMs,
                    binaryLogClient.getBinlogFilename(),
                    binaryLogClient.getBinlogPosition(),
                    communicationFailureCount,
                    deserializationFailureCount);
        } else {
            log.warn("Disconnect event received but was not connected");
        }

        connectionTimestamp = -1;
    }

    public boolean isConnected() {
        return connected;
    }

    public long getConnectionTimestamp() {
        return connectionTimestamp;
    }

    public int getCommunicationFailureCount() {
        return communicationFailureCount;
    }

    public int getDeserializationFailureCount() {
        return deserializationFailureCount;
    }

    public long getUptimeMs() {
        if (connectionTimestamp > 0 && connected) {
            return System.currentTimeMillis() - connectionTimestamp;
        }
        return 0;
    }
}
