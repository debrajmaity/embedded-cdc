package core;

import events.Event;
import events.EventType;

/**
 * Abstract interface for CDC (Change Data Capture) clients.
 * Implementations should handle database-specific CDC mechanisms.
 */
public interface CDCClient {

    /**
     * Connect to the database and start capturing change events.
     */
    void connect() throws Exception;

    /**
     * Disconnect from the database and stop capturing events.
     */
    void disconnect() throws Exception;

    /**
     * Check if the client is currently connected.
     * @return true if connected, false otherwise
     */
    boolean isConnected();

    /**
     * Register an event handler for processing CDC events.
     * @param handler the event handler to register
     */
    void registerEventHandler(CDCEventHandler handler);

    /**
     * Get the database type this client supports.
     * @return the database type identifier
     */
    String getDatabaseType();

    /**
     * Functional interface for handling CDC events.
     */
    @FunctionalInterface
    interface CDCEventHandler {
        void onEvent(Event event, EventType eventType);
    }
}
