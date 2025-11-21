package postgresql;

import core.CDCClient;
import events.Event;
import events.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.sql.*;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * PostgreSQL CDC Client using logical replication.
 * Uses the pgoutput plugin for capturing change events.
 */
public class PostgreSQLCDCClient implements CDCClient {

    private static final Logger logger = LoggerFactory.getLogger(PostgreSQLCDCClient.class);
    private static final String DATABASE_TYPE = "postgresql";

    private final PostgreSQLConfig config;
    private final List<CDCEventHandler> eventHandlers = new CopyOnWriteArrayList<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean connected = new AtomicBoolean(false);

    // Table metadata cache: schema.table -> column names
    private final Map<String, List<String>> columnNamesCache = new HashMap<>();
    private final Map<String, List<Integer>> columnTypesCache = new HashMap<>();

    private Connection replicationConnection;
    private Connection metadataConnection;
    private Thread replicationThread;

    // Current LSN for acknowledging
    private long currentLsn = 0;

    public PostgreSQLCDCClient(PostgreSQLConfig config) {
        this.config = config;
    }

    @Override
    public void connect() throws Exception {
        config.validate();
        logger.info("Connecting to PostgreSQL at {}:{}/{}",
            config.getHostname(), config.getPort(), config.getDatabase());

        // Create metadata connection for schema queries
        metadataConnection = DriverManager.getConnection(
            config.getJdbcUrl(),
            config.getUsername(),
            config.getPassword()
        );

        // Setup replication slot and publication if needed
        setupReplication();

        // Create replication connection
        Properties replicationProps = new Properties();
        replicationProps.setProperty("user", config.getUsername());
        replicationProps.setProperty("password", config.getPassword());
        replicationProps.setProperty("replication", "database");
        replicationProps.setProperty("assumeMinServerVersion", "10");
        replicationProps.setProperty("preferQueryMode", "simple");

        replicationConnection = DriverManager.getConnection(
            config.getReplicationUrl(),
            replicationProps
        );

        connected.set(true);
        logger.info("Connected to PostgreSQL successfully");

        // Start the replication stream
        startReplicationStream();
    }

    @Override
    public void disconnect() throws Exception {
        logger.info("Disconnecting from PostgreSQL");
        running.set(false);
        connected.set(false);

        if (replicationThread != null) {
            replicationThread.interrupt();
            try {
                replicationThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        closeConnection(replicationConnection);
        closeConnection(metadataConnection);

        logger.info("Disconnected from PostgreSQL");
    }

    @Override
    public boolean isConnected() {
        return connected.get();
    }

    @Override
    public void registerEventHandler(CDCEventHandler handler) {
        eventHandlers.add(handler);
    }

    @Override
    public String getDatabaseType() {
        return DATABASE_TYPE;
    }

    /**
     * Set up replication slot and publication if they don't exist.
     */
    private void setupReplication() throws SQLException {
        // Check and create replication slot
        if (!replicationSlotExists()) {
            createReplicationSlot();
        }

        // Check and create publication
        if (!publicationExists()) {
            createPublication();
        }

        // Load table metadata
        loadTableMetadata();
    }

    private boolean replicationSlotExists() throws SQLException {
        String sql = "SELECT 1 FROM pg_replication_slots WHERE slot_name = ?";
        try (PreparedStatement stmt = metadataConnection.prepareStatement(sql)) {
            stmt.setString(1, config.getSlotName());
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next();
            }
        }
    }

    private void createReplicationSlot() throws SQLException {
        String sql = String.format(
            "SELECT pg_create_logical_replication_slot('%s', '%s')",
            config.getSlotName(),
            config.getDecodingPlugin()
        );
        logger.info("Creating replication slot: {}", config.getSlotName());
        try (Statement stmt = metadataConnection.createStatement()) {
            stmt.execute(sql);
        }
        logger.info("Replication slot created successfully");
    }

    private boolean publicationExists() throws SQLException {
        String sql = "SELECT 1 FROM pg_publication WHERE pubname = ?";
        try (PreparedStatement stmt = metadataConnection.prepareStatement(sql)) {
            stmt.setString(1, config.getPublicationName());
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next();
            }
        }
    }

    private void createPublication() throws SQLException {
        String tables = config.getIncludeTables();
        String sql;
        if (tables == null || tables.isEmpty()) {
            sql = String.format("CREATE PUBLICATION %s FOR ALL TABLES", config.getPublicationName());
        } else {
            sql = String.format("CREATE PUBLICATION %s FOR TABLE %s", config.getPublicationName(), tables);
        }
        logger.info("Creating publication: {}", config.getPublicationName());
        try (Statement stmt = metadataConnection.createStatement()) {
            stmt.execute(sql);
        }
        logger.info("Publication created successfully");
    }

    /**
     * Load column metadata for all tables in the schema.
     */
    private void loadTableMetadata() throws SQLException {
        String sql = """
            SELECT table_schema, table_name, column_name, data_type, ordinal_position
            FROM information_schema.columns
            WHERE table_schema = ?
            ORDER BY table_schema, table_name, ordinal_position
            """;

        try (PreparedStatement stmt = metadataConnection.prepareStatement(sql)) {
            stmt.setString(1, config.getSchema());
            try (ResultSet rs = stmt.executeQuery()) {
                String currentTable = null;
                List<String> columns = new ArrayList<>();
                List<Integer> types = new ArrayList<>();

                while (rs.next()) {
                    String schema = rs.getString("table_schema");
                    String table = rs.getString("table_name");
                    String columnName = rs.getString("column_name");
                    String dataType = rs.getString("data_type");
                    String fullTableName = schema + "." + table;

                    if (currentTable != null && !currentTable.equals(fullTableName)) {
                        columnNamesCache.put(currentTable, new ArrayList<>(columns));
                        columnTypesCache.put(currentTable, new ArrayList<>(types));
                        columns.clear();
                        types.clear();
                    }

                    currentTable = fullTableName;
                    columns.add(columnName);
                    types.add(mapDataType(dataType));
                }

                if (currentTable != null && !columns.isEmpty()) {
                    columnNamesCache.put(currentTable, columns);
                    columnTypesCache.put(currentTable, types);
                }
            }
        }
        logger.info("Loaded metadata for {} tables", columnNamesCache.size());
    }

    private int mapDataType(String dataType) {
        return switch (dataType.toLowerCase()) {
            case "integer", "int", "int4" -> Types.INTEGER;
            case "bigint", "int8" -> Types.BIGINT;
            case "smallint", "int2" -> Types.SMALLINT;
            case "numeric", "decimal" -> Types.NUMERIC;
            case "real", "float4" -> Types.REAL;
            case "double precision", "float8" -> Types.DOUBLE;
            case "boolean", "bool" -> Types.BOOLEAN;
            case "character varying", "varchar", "text" -> Types.VARCHAR;
            case "character", "char" -> Types.CHAR;
            case "timestamp", "timestamp without time zone" -> Types.TIMESTAMP;
            case "timestamp with time zone" -> Types.TIMESTAMP_WITH_TIMEZONE;
            case "date" -> Types.DATE;
            case "time", "time without time zone" -> Types.TIME;
            case "bytea" -> Types.BINARY;
            case "json", "jsonb" -> Types.OTHER;
            case "uuid" -> Types.OTHER;
            default -> Types.OTHER;
        };
    }

    /**
     * Start the logical replication stream.
     */
    private void startReplicationStream() {
        running.set(true);
        replicationThread = new Thread(() -> {
            try {
                streamChanges();
            } catch (Exception e) {
                if (running.get()) {
                    logger.error("Error in replication stream", e);
                }
            }
        }, "postgresql-cdc-stream");
        replicationThread.setDaemon(true);
        replicationThread.start();
        logger.info("Replication stream started");
    }

    /**
     * Stream changes using pg_logical_slot_get_changes for simplicity.
     * For production, use the streaming replication protocol.
     */
    private void streamChanges() throws SQLException, InterruptedException {
        logger.info("Starting change streaming from slot: {}", config.getSlotName());

        while (running.get()) {
            try {
                pollChanges();
                Thread.sleep(100); // Poll interval
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (SQLException e) {
                if (running.get()) {
                    logger.error("Error polling changes", e);
                    Thread.sleep(1000); // Backoff on error
                }
            }
        }
    }

    /**
     * Poll for changes using SQL function.
     */
    private void pollChanges() throws SQLException {
        String sql = String.format(
            "SELECT lsn, xid, data FROM pg_logical_slot_get_changes('%s', NULL, NULL, 'proto_version', '1', 'publication_names', '%s')",
            config.getSlotName(),
            config.getPublicationName()
        );

        try (Statement stmt = metadataConnection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                String lsnStr = rs.getString("lsn");
                byte[] data = rs.getBytes("data");

                if (data != null && data.length > 0) {
                    processPgOutputMessage(data);
                }
            }
        }
    }

    /**
     * Process a pgoutput protocol message.
     */
    private void processPgOutputMessage(byte[] data) {
        if (data == null || data.length == 0) {
            return;
        }

        ByteBuffer buffer = ByteBuffer.wrap(data);
        char messageType = (char) buffer.get();

        try {
            switch (messageType) {
                case 'B' -> processBeginMessage(buffer);
                case 'C' -> processCommitMessage(buffer);
                case 'I' -> processInsertMessage(buffer);
                case 'U' -> processUpdateMessage(buffer);
                case 'D' -> processDeleteMessage(buffer);
                case 'R' -> processRelationMessage(buffer);
                case 'T' -> processTruncateMessage(buffer);
                default -> logger.trace("Unknown message type: {}", messageType);
            }
        } catch (Exception e) {
            logger.error("Error processing message type {}: {}", messageType, e.getMessage());
        }
    }

    private void processBeginMessage(ByteBuffer buffer) {
        logger.trace("Transaction begin");
    }

    private void processCommitMessage(ByteBuffer buffer) {
        logger.trace("Transaction commit");
    }

    private void processRelationMessage(ByteBuffer buffer) {
        // Relation messages contain table metadata
        // Update cache if needed
        logger.trace("Relation message received");
    }

    private void processTruncateMessage(ByteBuffer buffer) {
        logger.debug("Truncate message received");
    }

    private void processInsertMessage(ByteBuffer buffer) {
        try {
            int relationId = buffer.getInt();
            char newTupleType = (char) buffer.get();

            if (newTupleType == 'N') {
                Map<String, Object> values = parseTupleData(buffer, relationId);
                String tableName = getTableName(relationId);

                Event event = createEvent(tableName, null, values);
                notifyHandlers(event, EventType.INSERT);
            }
        } catch (Exception e) {
            logger.error("Error processing INSERT message", e);
        }
    }

    private void processUpdateMessage(ByteBuffer buffer) {
        try {
            int relationId = buffer.getInt();
            char tupleType = (char) buffer.get();

            Map<String, Object> oldValues = null;
            Map<String, Object> newValues = null;

            // Check for old tuple (K or O)
            if (tupleType == 'K' || tupleType == 'O') {
                oldValues = parseTupleData(buffer, relationId);
                tupleType = (char) buffer.get();
            }

            // New tuple (N)
            if (tupleType == 'N') {
                newValues = parseTupleData(buffer, relationId);
            }

            String tableName = getTableName(relationId);
            Event event = createEvent(tableName, oldValues, newValues);
            notifyHandlers(event, EventType.UPDATE);
        } catch (Exception e) {
            logger.error("Error processing UPDATE message", e);
        }
    }

    private void processDeleteMessage(ByteBuffer buffer) {
        try {
            int relationId = buffer.getInt();
            char tupleType = (char) buffer.get();

            Map<String, Object> oldValues = null;
            if (tupleType == 'K' || tupleType == 'O') {
                oldValues = parseTupleData(buffer, relationId);
            }

            String tableName = getTableName(relationId);
            Event event = createEvent(tableName, oldValues, null);
            notifyHandlers(event, EventType.DELETE);
        } catch (Exception e) {
            logger.error("Error processing DELETE message", e);
        }
    }

    private Map<String, Object> parseTupleData(ByteBuffer buffer, int relationId) {
        Map<String, Object> values = new LinkedHashMap<>();

        short numColumns = buffer.getShort();
        String tableName = getTableName(relationId);
        List<String> columnNames = columnNamesCache.getOrDefault(tableName, Collections.emptyList());

        for (int i = 0; i < numColumns; i++) {
            char columnType = (char) buffer.get();
            String columnName = i < columnNames.size() ? columnNames.get(i) : "column_" + i;

            Object value = switch (columnType) {
                case 'n' -> null; // NULL
                case 'u' -> null; // Unchanged TOAST
                case 't' -> {
                    // Text value
                    int length = buffer.getInt();
                    byte[] data = new byte[length];
                    buffer.get(data);
                    yield new String(data);
                }
                case 'b' -> {
                    // Binary value
                    int length = buffer.getInt();
                    byte[] data = new byte[length];
                    buffer.get(data);
                    yield data;
                }
                default -> null;
            };

            values.put(columnName, value);
        }

        return values;
    }

    private String getTableName(int relationId) {
        // In a full implementation, maintain a relation ID to table name mapping
        // For now, return a placeholder
        return config.getSchema() + ".table_" + relationId;
    }

    private Event createEvent(String fullTableName, Map<String, Object> before, Map<String, Object> after) {
        Event event = new Event();

        String[] parts = fullTableName.split("\\.", 2);
        if (parts.length == 2) {
            event.setDatabase(parts[0]);
            event.setTable(parts[1]);
        } else {
            event.setDatabase(config.getDatabase());
            event.setTable(fullTableName);
        }

        List<String> columns = columnNamesCache.getOrDefault(fullTableName, Collections.emptyList());
        event.setColumnList(new ArrayList<>(columns));
        event.setChangesBefore(before);
        event.setChangesAfter(after);

        return event;
    }

    private void notifyHandlers(Event event, EventType eventType) {
        for (CDCEventHandler handler : eventHandlers) {
            try {
                handler.onEvent(event, eventType);
            } catch (Exception e) {
                logger.error("Error in event handler", e);
            }
        }
    }

    private void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                logger.warn("Error closing connection", e);
            }
        }
    }

    /**
     * Get column names for a specific table.
     */
    public List<String> getColumnNames(String schema, String table) {
        return columnNamesCache.getOrDefault(schema + "." + table, Collections.emptyList());
    }

    /**
     * Refresh metadata for all tables.
     */
    public void refreshMetadata() throws SQLException {
        columnNamesCache.clear();
        columnTypesCache.clear();
        loadTableMetadata();
    }

    /**
     * Drop the replication slot (cleanup).
     */
    public void dropReplicationSlot() throws SQLException {
        if (metadataConnection != null && !metadataConnection.isClosed()) {
            String sql = String.format("SELECT pg_drop_replication_slot('%s')", config.getSlotName());
            try (Statement stmt = metadataConnection.createStatement()) {
                stmt.execute(sql);
            }
            logger.info("Replication slot {} dropped", config.getSlotName());
        }
    }

    /**
     * Drop the publication (cleanup).
     */
    public void dropPublication() throws SQLException {
        if (metadataConnection != null && !metadataConnection.isClosed()) {
            String sql = String.format("DROP PUBLICATION IF EXISTS %s", config.getPublicationName());
            try (Statement stmt = metadataConnection.createStatement()) {
                stmt.execute(sql);
            }
            logger.info("Publication {} dropped", config.getPublicationName());
        }
    }
}
