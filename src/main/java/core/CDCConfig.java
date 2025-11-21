package core;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Base configuration class for CDC clients.
 * Provides common configuration properties shared across database implementations.
 */
public abstract class CDCConfig {

    protected Properties properties;
    protected String hostname;
    protected int port;
    protected String username;
    protected String password;
    protected String database;

    // Connection settings
    protected boolean keepAlive = true;
    protected long keepAliveInterval = 60000;
    protected long heartbeatInterval = 30000;

    /**
     * Load configuration from properties.
     * @param properties the properties to load from
     */
    protected abstract void loadFromProperties(Properties properties);

    /**
     * Get the database type identifier.
     * @return database type string
     */
    public abstract String getDatabaseType();

    /**
     * Validate the configuration.
     * @throws IllegalArgumentException if configuration is invalid
     */
    public void validate() {
        if (hostname == null || hostname.isEmpty()) {
            throw new IllegalArgumentException("Hostname is required");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Port must be between 1 and 65535");
        }
        if (username == null || username.isEmpty()) {
            throw new IllegalArgumentException("Username is required");
        }
    }

    /**
     * Load configuration from a properties file.
     * @param resourcePath path to the properties file
     * @throws IOException if the file cannot be read
     */
    protected void loadFromResource(String resourcePath) throws IOException {
        properties = new Properties();
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
            if (is != null) {
                properties.load(is);
                loadFromProperties(properties);
            }
        }
    }

    // Common getters and setters

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public boolean isKeepAlive() {
        return keepAlive;
    }

    public void setKeepAlive(boolean keepAlive) {
        this.keepAlive = keepAlive;
    }

    public long getKeepAliveInterval() {
        return keepAliveInterval;
    }

    public void setKeepAliveInterval(long keepAliveInterval) {
        this.keepAliveInterval = keepAliveInterval;
    }

    public long getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public void setHeartbeatInterval(long heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    protected String getProperty(String key, String defaultValue) {
        return properties != null ? properties.getProperty(key, defaultValue) : defaultValue;
    }

    protected int getIntProperty(String key, int defaultValue) {
        String value = getProperty(key, null);
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    protected long getLongProperty(String key, long defaultValue) {
        String value = getProperty(key, null);
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    protected boolean getBooleanProperty(String key, boolean defaultValue) {
        String value = getProperty(key, null);
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value.trim());
    }
}
