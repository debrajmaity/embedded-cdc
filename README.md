# Embedded CDC - Multi-Database Change Data Capture

A lightweight, embedded Change Data Capture (CDC) tool that captures database changes in real-time. Supports **MySQL** and **PostgreSQL**.

## Features

- Real-time capture of INSERT, UPDATE, and DELETE operations
- **Multi-database support**: MySQL and PostgreSQL
- Configurable connection settings via properties files
- Automatic position tracking (binlog for MySQL, LSN for PostgreSQL)
- Comprehensive error handling and logging
- Graceful shutdown handling
- Extensible event processing architecture

## Supported Databases

| Database   | Version   | CDC Method                  |
|------------|-----------|----------------------------|
| MySQL      | 5.7+, 8.0+| Binary Log (Row-based)     |
| PostgreSQL | 10+       | Logical Replication        |

## Prerequisites

- Java 11 or higher
- Maven 3.6+
- Database with CDC enabled (see configuration below)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/debrajmaity/embedded-cdc.git
cd embedded-cdc
```

2. Build the project:
```bash
mvn clean package
```

---

## MySQL Setup

### Enable Binary Logging

Add to your MySQL `my.cnf`:

```ini
[mysqld]
server-id = 1
log_bin = mysql-bin
binlog_format = ROW
binlog_row_image = FULL
```

### Create CDC User

```sql
CREATE USER 'cdc_user'@'%' IDENTIFIED BY 'your_password';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';
FLUSH PRIVILEGES;
```

### MySQL Configuration

Edit `src/main/resources/binlog.properties`:

```properties
mysql.hostname=localhost
mysql.port=3306
mysql.username=cdc_user
mysql.password=your_password

binlog.filename=
binlog.position=

binlog.keepAlive=true
binlog.keepAliveInterval=60000
binlog.heartbeatInterval=30000
```

### MySQL Usage

```java
// Using BinLogReader directly
BinLogReader reader = new BinLogReader();
reader.start();

// Or using unified CDCReader
CDCReader reader = new CDCReader("mysql", "binlog.properties");
reader.start();
```

---

## PostgreSQL Setup

### Enable Logical Replication

In `postgresql.conf`:

```ini
wal_level = logical
max_replication_slots = 4
max_wal_senders = 4
```

In `pg_hba.conf` (allow replication connections):

```
host    replication     all     127.0.0.1/32    md5
host    replication     all     ::1/128         md5
```

Restart PostgreSQL after configuration changes.

### Create CDC User

```sql
-- Create user with replication privileges
CREATE USER cdc_user WITH REPLICATION LOGIN PASSWORD 'your_password';

-- Grant necessary permissions
GRANT CONNECT ON DATABASE your_database TO cdc_user;
GRANT USAGE ON SCHEMA public TO cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO cdc_user;
```

### PostgreSQL Configuration

Edit `src/main/resources/postgresql.properties`:

```properties
# Connection
postgres.hostname=localhost
postgres.port=5432
postgres.username=cdc_user
postgres.password=your_password
postgres.database=your_database
postgres.schema=public

# Replication
postgres.slotName=embedded_cdc_slot
postgres.publicationName=embedded_cdc_publication
postgres.decodingPlugin=pgoutput

# Optional: Table filtering
postgres.includeTables=users,orders
postgres.excludeTables=

# Connection settings
postgres.keepAlive=true
postgres.keepAliveInterval=60000
postgres.heartbeatInterval=30000
```

### PostgreSQL Usage

```java
// Using PostgreSQLReader directly
PostgreSQLReader reader = new PostgreSQLReader();
reader.start();

// With custom configuration
PostgreSQLConfig config = new PostgreSQLConfig();
config.setHostname("localhost");
config.setPort(5432);
config.setUsername("cdc_user");
config.setPassword("your_password");
config.setDatabase("mydb");

PostgreSQLReader reader = new PostgreSQLReader(config);
reader.start();

// Or using unified CDCReader
CDCReader reader = new CDCReader("postgresql", "postgresql.properties");
reader.start();
```

---

## Unified Usage

Use `CDCReader` for database-agnostic code:

```java
// From configuration file (specify database.type in cdc.properties)
CDCReader reader = new CDCReader("cdc.properties");
reader.start();

// Specify database type explicitly
CDCReader reader = new CDCReader("postgresql", "postgresql.properties");
reader.start();
```

### Running from Command Line

```bash
# MySQL (default)
java -jar target/embedded-cdc-1.0-SNAPSHOT.jar

# PostgreSQL
java -cp target/embedded-cdc-1.0-SNAPSHOT.jar postgresql.PostgreSQLReader

# Using unified reader
java -cp target/embedded-cdc-1.0-SNAPSHOT.jar core.CDCReader postgresql postgresql.properties
```

---

## Extending Event Processing

### Override processEvent

```java
// For MySQL
public class CustomMySQLListener extends BinLogEventListener {
    @Override
    protected void processEvent(Event event, EventType eventType) {
        // Send to Kafka, save to database, etc.
        System.out.println("Event: " + eventType + " on " +
            event.getDatabase() + "." + event.getTable());
    }
}

// For PostgreSQL
public class CustomPostgreSQLReader extends PostgreSQLReader {
    @Override
    protected void processEvent(Event event, EventType eventType) {
        // Custom handling
        sendToKafka(event, eventType);
    }
}
```

### Register Event Handler

```java
CDCReader reader = new CDCReader("postgresql", "postgresql.properties");
reader.registerEventHandler((event, eventType) -> {
    System.out.println("Captured: " + eventType + " on " + event.getTable());
    System.out.println("Before: " + event.getChangesBefore());
    System.out.println("After: " + event.getChangesAfter());
});
reader.start();
```

---

## Architecture

```
┌────────────────────────────────────────────────────────────┐
│                        CDCReader                           │
│                   (Unified Entry Point)                    │
└─────────────────────────┬──────────────────────────────────┘
                          │
            ┌─────────────┴─────────────┐
            │                           │
┌───────────▼───────────┐   ┌───────────▼───────────┐
│    MySQLCDCClient     │   │  PostgreSQLCDCClient  │
│   (BinaryLogClient)   │   │ (Logical Replication) │
└───────────┬───────────┘   └───────────┬───────────┘
            │                           │
            └─────────────┬─────────────┘
                          │
              ┌───────────▼───────────┐
              │     Event (DTO)       │
              │ - database            │
              │ - table               │
              │ - columnList          │
              │ - changesBefore       │
              │ - changesAfter        │
              └───────────────────────┘
```

## Project Structure

```
embedded-cdc/
├── src/main/java/
│   ├── BinLogReader.java              # MySQL main class
│   ├── core/
│   │   ├── CDCClient.java             # Abstract CDC client interface
│   │   ├── CDCConfig.java             # Base configuration class
│   │   └── CDCReader.java             # Unified CDC reader
│   ├── config/
│   │   └── BinLogConfig.java          # MySQL configuration
│   ├── events/
│   │   ├── Event.java                 # Event data model
│   │   └── EventType.java             # Event type enum (INSERT, UPDATE, DELETE)
│   ├── listeners/
│   │   ├── BinLogEventListener.java   # MySQL event processing
│   │   └── BinLogClientLifeCycleListener.java
│   ├── parser/
│   │   ├── RowDataParser.java         # Parser interface
│   │   ├── InsertDataParser.java
│   │   ├── UpdateRowDataParser.java
│   │   └── DeleteDataParser.java
│   └── postgresql/
│       ├── PostgreSQLConfig.java      # PostgreSQL configuration
│       ├── PostgreSQLCDCClient.java   # PostgreSQL CDC client
│       └── PostgreSQLReader.java      # PostgreSQL main class
└── src/main/resources/
    ├── binlog.properties              # MySQL configuration
    ├── postgresql.properties          # PostgreSQL configuration
    ├── cdc.properties                 # Unified configuration
    └── log4j2.xml                     # Logging configuration
```

## Dependencies

- MySQL Connector/J 8.0.33
- PostgreSQL JDBC Driver 42.7.1
- Zendesk MySQL Binlog Connector 0.29.2
- Log4j2 2.20.0
- SLF4J (via Log4j2)

## Troubleshooting

### MySQL Issues

- Verify binary logging: `SHOW VARIABLES LIKE 'log_bin';`
- Check format: `SHOW VARIABLES LIKE 'binlog_format';`
- Verify permissions: `SHOW GRANTS FOR 'cdc_user'@'%';`

### PostgreSQL Issues

- Check wal_level: `SHOW wal_level;` (should be 'logical')
- List replication slots: `SELECT * FROM pg_replication_slots;`
- List publications: `SELECT * FROM pg_publication;`
- Check pg_hba.conf for replication connections

### Common Issues

- **Connection refused**: Check hostname, port, firewall settings
- **Permission denied**: Verify user privileges
- **No events captured**: Ensure CDC is enabled and tables are included

## Cleanup (PostgreSQL)

To remove replication slot and publication:

```sql
-- Drop replication slot
SELECT pg_drop_replication_slot('embedded_cdc_slot');

-- Drop publication
DROP PUBLICATION IF EXISTS embedded_cdc_publication;
```

## Future Enhancements

- [x] PostgreSQL support
- [ ] Add unit and integration tests
- [ ] Implement table filtering
- [ ] Add position checkpointing to external storage
- [ ] Implement retry logic with exponential backoff
- [ ] Add metrics and monitoring (JMX/Prometheus)
- [ ] Support for multiple output formats (JSON, Avro, Protobuf)
- [ ] Kafka/RabbitMQ integration
- [ ] Docker containerization
- [ ] Oracle database support
- [ ] SQL Server support

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

See LICENSE file for details.
