package listeners;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import events.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parser.DeleteDataParser;
import parser.InsertDataParser;
import parser.UpdateRowDataParser;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BinLogEventListener implements BinaryLogClient.EventListener {
    private static final Logger log = LoggerFactory.getLogger(BinLogEventListener.class);

    // Cache for table metadata (tableId -> TableMapEventData)
    private final Map<Long, TableMapEventData> tableMapCache = new HashMap<>();

    // Cache for column names (database.table -> List<String>)
    private final Map<String, List<String>> columnNamesCache = new HashMap<>();

    @Override
    public void onEvent(com.github.shyiko.mysql.binlog.event.Event event) {
        EventData eventData = event.getData();

        try {
            if (eventData instanceof TableMapEventData) {
                handleTableMapEvent((TableMapEventData) eventData);
            } else if (eventData instanceof WriteRowsEventData) {
                handleInsertEvent((WriteRowsEventData) eventData);
            } else if (eventData instanceof UpdateRowsEventData) {
                handleUpdateEvent((UpdateRowsEventData) eventData);
            } else if (eventData instanceof DeleteRowsEventData) {
                handleDeleteEvent((DeleteRowsEventData) eventData);
            } else {
                log.debug("Unhandled event type: {}", event.getHeader().getEventType());
            }
        } catch (Exception e) {
            log.error("Error processing binlog event", e);
        }
    }

    private void handleTableMapEvent(TableMapEventData eventData) {
        tableMapCache.put(eventData.getTableId(), eventData);
        log.debug("Cached table map for tableId={}, database={}, table={}",
                eventData.getTableId(), eventData.getDatabase(), eventData.getTable());
    }

    private void handleInsertEvent(WriteRowsEventData eventData) {
        TableMapEventData tableMapData = tableMapCache.get(eventData.getTableId());
        if (tableMapData == null) {
            log.warn("No table map found for tableId={}", eventData.getTableId());
            return;
        }

        String database = tableMapData.getDatabase();
        String table = tableMapData.getTable();
        List<String> columnNames = getColumnNames(database, table);

        InsertDataParser parser = new InsertDataParser(columnNames);
        Event parsedEvent = parser.parse(eventData, database, table);

        if (parsedEvent != null) {
            processEvent(parsedEvent, events.EventType.INSERT);
        }
    }

    private void handleUpdateEvent(UpdateRowsEventData eventData) {
        TableMapEventData tableMapData = tableMapCache.get(eventData.getTableId());
        if (tableMapData == null) {
            log.warn("No table map found for tableId={}", eventData.getTableId());
            return;
        }

        String database = tableMapData.getDatabase();
        String table = tableMapData.getTable();
        List<String> columnNames = getColumnNames(database, table);

        UpdateRowDataParser parser = new UpdateRowDataParser(columnNames);
        Event parsedEvent = parser.parse(eventData, database, table);

        if (parsedEvent != null) {
            processEvent(parsedEvent, events.EventType.UPDATE);
        }
    }

    private void handleDeleteEvent(DeleteRowsEventData eventData) {
        TableMapEventData tableMapData = tableMapCache.get(eventData.getTableId());
        if (tableMapData == null) {
            log.warn("No table map found for tableId={}", eventData.getTableId());
            return;
        }

        String database = tableMapData.getDatabase();
        String table = tableMapData.getTable();
        List<String> columnNames = getColumnNames(database, table);

        DeleteDataParser parser = new DeleteDataParser(columnNames);
        Event parsedEvent = parser.parse(eventData, database, table);

        if (parsedEvent != null) {
            processEvent(parsedEvent, events.EventType.DELETE);
        }
    }

    /**
     * Get column names for a table. Override this method to provide actual column metadata.
     * Currently returns a placeholder implementation.
     */
    private List<String> getColumnNames(String database, String table) {
        String key = database + "." + table;
        return columnNamesCache.computeIfAbsent(key, k -> {
            // TODO: Implement actual column name retrieval from MySQL metadata
            log.warn("Column names not cached for {}.{}, using empty list", database, table);
            return List.of();
        });
    }

    /**
     * Process the parsed event. Override this method to implement custom event handling
     * (e.g., send to Kafka, save to database, etc.)
     */
    protected void processEvent(Event event, events.EventType eventType) {
        log.info("Processed {} event - Database: {}, Table: {}, Before: {}, After: {}",
                eventType.getValue(),
                event.getDatabase(),
                event.getTable(),
                event.getChangesBefore(),
                event.getChangesAfter());
    }

    /**
     * Set column names for a specific table. Call this method to populate the column cache.
     */
    public void setColumnNames(String database, String table, List<String> columnNames) {
        String key = database + "." + table;
        columnNamesCache.put(key, columnNames);
        log.debug("Cached column names for {}.{}: {}", database, table, columnNames);
    }

    /**
     * Clear all cached metadata. Useful for reconnection scenarios.
     */
    public void clearCache() {
        tableMapCache.clear();
        columnNamesCache.clear();
        log.debug("Cleared all event listener caches");
    }
}
