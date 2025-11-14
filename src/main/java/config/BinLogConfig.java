package config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Configuration manager for BinLog reader settings.
 * Loads configuration from binlog.properties file.
 */
public class BinLogConfig {
    private static final Logger log = LoggerFactory.getLogger(BinLogConfig.class);
    private static final String CONFIG_FILE = "binlog.properties";

    private final Properties properties;

    public BinLogConfig() {
        this.properties = new Properties();
        loadConfig();
    }

    public BinLogConfig(String configFile) {
        this.properties = new Properties();
        loadConfig(configFile);
    }

    private void loadConfig() {
        loadConfig(CONFIG_FILE);
    }

    private void loadConfig(String configFile) {
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(configFile)) {
            if (input == null) {
                log.warn("Configuration file '{}' not found, using defaults", configFile);
                loadDefaults();
                return;
            }
            properties.load(input);
            log.info("Loaded configuration from {}", configFile);
        } catch (IOException e) {
            log.error("Error loading configuration file: {}", configFile, e);
            loadDefaults();
        }
    }

    private void loadDefaults() {
        properties.setProperty("mysql.hostname", "localhost");
        properties.setProperty("mysql.port", "3306");
        properties.setProperty("mysql.username", "root");
        properties.setProperty("mysql.password", "");
        properties.setProperty("binlog.position", "");
        properties.setProperty("binlog.filename", "");
        properties.setProperty("binlog.keepAlive", "true");
        properties.setProperty("binlog.keepAliveInterval", "60000");
        properties.setProperty("binlog.heartbeatInterval", "30000");
    }

    public String getHostname() {
        return properties.getProperty("mysql.hostname", "localhost");
    }

    public int getPort() {
        String port = properties.getProperty("mysql.port", "3306");
        try {
            return Integer.parseInt(port);
        } catch (NumberFormatException e) {
            log.warn("Invalid port number: {}, using default 3306", port);
            return 3306;
        }
    }

    public String getUsername() {
        return properties.getProperty("mysql.username", "root");
    }

    public String getPassword() {
        return properties.getProperty("mysql.password", "");
    }

    public String getBinlogFilename() {
        return properties.getProperty("binlog.filename", "");
    }

    public long getBinlogPosition() {
        String position = properties.getProperty("binlog.position", "");
        if (position.isEmpty()) {
            return -1;
        }
        try {
            return Long.parseLong(position);
        } catch (NumberFormatException e) {
            log.warn("Invalid binlog position: {}, starting from current position", position);
            return -1;
        }
    }

    public boolean isKeepAlive() {
        return Boolean.parseBoolean(properties.getProperty("binlog.keepAlive", "true"));
    }

    public long getKeepAliveInterval() {
        String interval = properties.getProperty("binlog.keepAliveInterval", "60000");
        try {
            return Long.parseLong(interval);
        } catch (NumberFormatException e) {
            log.warn("Invalid keepAlive interval: {}, using default 60000", interval);
            return 60000;
        }
    }

    public long getHeartbeatInterval() {
        String interval = properties.getProperty("binlog.heartbeatInterval", "30000");
        try {
            return Long.parseLong(interval);
        } catch (NumberFormatException e) {
            log.warn("Invalid heartbeat interval: {}, using default 30000", interval);
            return 30000;
        }
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    public String getProperty(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }
}
