package postgresql;

import core.CDCConfig;

import java.io.IOException;
import java.util.Properties;

/**
 * Configuration class for PostgreSQL CDC client.
 * Handles PostgreSQL-specific settings for logical replication.
 */
public class PostgreSQLConfig extends CDCConfig {

    private static final String DEFAULT_CONFIG_FILE = "postgresql.properties";
    private static final String DATABASE_TYPE = "postgresql";

    // PostgreSQL-specific settings
    private String slotName = "embedded_cdc_slot";
    private String publicationName = "embedded_cdc_publication";
    private String decodingPlugin = "pgoutput";
    private String schema = "public";

    // LSN (Log Sequence Number) position
    private String startLsn;

    // Tables to monitor (comma-separated, empty means all)
    private String includeTables = "";
    private String excludeTables = "";

    /**
     * Create configuration with default settings.
     */
    public PostgreSQLConfig() {
        this.hostname = "localhost";
        this.port = 5432;
        this.username = "postgres";
        this.password = "";
        this.database = "postgres";
    }

    /**
     * Create configuration from properties file.
     * @param configFile path to the configuration file
     * @throws IOException if the file cannot be read
     */
    public PostgreSQLConfig(String configFile) throws IOException {
        loadFromResource(configFile != null ? configFile : DEFAULT_CONFIG_FILE);
    }

    /**
     * Create configuration with explicit values.
     */
    public PostgreSQLConfig(String hostname, int port, String username, String password, String database) {
        this.hostname = hostname;
        this.port = port;
        this.username = username;
        this.password = password;
        this.database = database;
    }

    @Override
    protected void loadFromProperties(Properties properties) {
        this.hostname = getProperty("postgres.hostname", "localhost");
        this.port = getIntProperty("postgres.port", 5432);
        this.username = getProperty("postgres.username", "postgres");
        this.password = getProperty("postgres.password", "");
        this.database = getProperty("postgres.database", "postgres");

        // PostgreSQL-specific settings
        this.slotName = getProperty("postgres.slotName", "embedded_cdc_slot");
        this.publicationName = getProperty("postgres.publicationName", "embedded_cdc_publication");
        this.decodingPlugin = getProperty("postgres.decodingPlugin", "pgoutput");
        this.schema = getProperty("postgres.schema", "public");
        this.startLsn = getProperty("postgres.startLsn", null);

        // Table filtering
        this.includeTables = getProperty("postgres.includeTables", "");
        this.excludeTables = getProperty("postgres.excludeTables", "");

        // Connection settings
        this.keepAlive = getBooleanProperty("postgres.keepAlive", true);
        this.keepAliveInterval = getLongProperty("postgres.keepAliveInterval", 60000);
        this.heartbeatInterval = getLongProperty("postgres.heartbeatInterval", 30000);
    }

    @Override
    public String getDatabaseType() {
        return DATABASE_TYPE;
    }

    @Override
    public void validate() {
        super.validate();
        if (database == null || database.isEmpty()) {
            throw new IllegalArgumentException("Database name is required for PostgreSQL");
        }
        if (slotName == null || slotName.isEmpty()) {
            throw new IllegalArgumentException("Replication slot name is required");
        }
    }

    // PostgreSQL-specific getters and setters

    public String getSlotName() {
        return slotName;
    }

    public void setSlotName(String slotName) {
        this.slotName = slotName;
    }

    public String getPublicationName() {
        return publicationName;
    }

    public void setPublicationName(String publicationName) {
        this.publicationName = publicationName;
    }

    public String getDecodingPlugin() {
        return decodingPlugin;
    }

    public void setDecodingPlugin(String decodingPlugin) {
        this.decodingPlugin = decodingPlugin;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getStartLsn() {
        return startLsn;
    }

    public void setStartLsn(String startLsn) {
        this.startLsn = startLsn;
    }

    public String getIncludeTables() {
        return includeTables;
    }

    public void setIncludeTables(String includeTables) {
        this.includeTables = includeTables;
    }

    public String getExcludeTables() {
        return excludeTables;
    }

    public void setExcludeTables(String excludeTables) {
        this.excludeTables = excludeTables;
    }

    /**
     * Get the JDBC connection URL for PostgreSQL.
     * @return JDBC URL string
     */
    public String getJdbcUrl() {
        return String.format("jdbc:postgresql://%s:%d/%s", hostname, port, database);
    }

    /**
     * Get the replication connection URL for PostgreSQL.
     * @return replication URL string
     */
    public String getReplicationUrl() {
        return String.format("jdbc:postgresql://%s:%d/%s?replication=database", hostname, port, database);
    }
}
