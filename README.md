# Embedded CDC - MySQL Change Data Capture

A lightweight, embedded MySQL Change Data Capture (CDC) tool that reads MySQL binary logs and processes database change events in real-time.

## Features

- Real-time capture of MySQL INSERT, UPDATE, and DELETE operations
- Configurable connection settings via properties file
- Automatic binlog position tracking
- Comprehensive error handling and logging
- Graceful shutdown handling
- Extensible event processing architecture

## Prerequisites

- Java 11 or higher
- MySQL 5.7+ or MySQL 8.0+ with binary logging enabled
- Maven 3.6+

## MySQL Configuration

Enable binary logging in your MySQL server by adding to `my.cnf`:

```ini
[mysqld]
server-id = 1
log_bin = mysql-bin
binlog_format = ROW
binlog_row_image = FULL
```

Create a MySQL user with replication permissions:

```sql
CREATE USER 'cdc_user'@'%' IDENTIFIED BY 'your_password';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';
FLUSH PRIVILEGES;
```

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

## Configuration

Edit `src/main/resources/binlog.properties`:

```properties
# MySQL Connection Settings
mysql.hostname=localhost
mysql.port=3306
mysql.username=cdc_user
mysql.password=your_password

# Binlog Position (optional - leave empty to start from current position)
binlog.filename=
binlog.position=

# Connection Settings
binlog.keepAlive=true
binlog.keepAliveInterval=60000
binlog.heartbeatInterval=30000
```

## Usage

### Running the Application

```bash
java -jar target/embedded-cdc-1.0-SNAPSHOT.jar
```

Or using Maven:

```bash
mvn exec:java -Dexec.mainClass="BinLogReader"
```

### Programmatic Usage

```java
// Using configuration file
BinLogReader reader = new BinLogReader();
reader.start();

// ... application runs ...

reader.stop();
```

```java
// Using custom configuration
BinLogConfig config = new BinLogConfig();
BinLogReader reader = new BinLogReader(config);
reader.start();
```

### Extending Event Processing

Override the `processEvent` method in `BinLogEventListener` to implement custom event handling:

```java
public class CustomEventListener extends BinLogEventListener {
    @Override
    protected void processEvent(Event event, EventType eventType) {
        // Send to Kafka, save to database, etc.
        System.out.println("Event: " + eventType + " on " +
            event.getDatabase() + "." + event.getTable());
    }
}
```

## Architecture

```
┌─────────────────┐
│   BinLogReader  │  Main application class
└────────┬────────┘
         │
         ├─────────────────┐
         │                 │
┌────────▼────────┐ ┌─────▼──────────────┐
│ BinLogConfig    │ │ BinaryLogClient    │
│ (Configuration) │ │ (Zendesk library)  │
└─────────────────┘ └────────┬───────────┘
                             │
         ┌───────────────────┼───────────────────┐
         │                   │                   │
┌────────▼───────────┐ ┌────▼─────────────┐ ┌──▼───────────────┐
│ BinLogEventListener│ │LifeCycleListener │ │ EventDeserializer│
│ (Process events)   │ │ (Handle errors)  │ │                  │
└────────┬───────────┘ └──────────────────┘ └──────────────────┘
         │
         ├──────────────────┬──────────────────┐
         │                  │                  │
┌────────▼────────┐ ┌───────▼────────┐ ┌─────▼──────────┐
│ InsertDataParser│ │UpdateDataParser│ │DeleteDataParser│
└─────────────────┘ └────────────────┘ └────────────────┘
```

## Project Structure

```
embedded-cdc/
├── src/main/java/
│   ├── BinLogReader.java              # Main application class
│   ├── config/
│   │   └── BinLogConfig.java          # Configuration manager
│   ├── events/
│   │   ├── Event.java                 # Event data model
│   │   └── EventType.java             # Event type enum
│   ├── listeners/
│   │   ├── BinLogEventListener.java   # Event processing
│   │   └── BinLogClientLifeCycleListener.java # Connection lifecycle
│   └── parser/
│       ├── RowDataParser.java         # Parser interface
│       ├── InsertDataParser.java      # INSERT event parser
│       ├── UpdateRowDataParser.java   # UPDATE event parser
│       └── DeleteDataParser.java      # DELETE event parser
└── src/main/resources/
    ├── binlog.properties              # Configuration file
    └── log4j2.xml                     # Logging configuration
```

## Recent Improvements

### Security
- ✅ Updated MySQL Connector from 8.0.17 to 8.0.33
- ✅ Updated Log4j2 from 2.17.1 to 2.20.0 (fixes CVE vulnerabilities)
- ✅ Updated binlog connector to 0.29.2
- ✅ Upgraded Java from 1.8 to 11

### Code Quality
- ✅ Fixed Event class encapsulation (made fields private)
- ✅ Fixed logging statement in BinLogEventListener
- ✅ Added comprehensive error handling throughout
- ✅ Added input validation in BinLogReader
- ✅ Added JavaDoc documentation

### Features
- ✅ Implemented all parser logic (Insert/Update/Delete)
- ✅ Implemented event listener routing and parsing
- ✅ Added configuration management with properties file
- ✅ Added graceful shutdown handling
- ✅ Added connection lifecycle monitoring
- ✅ Added Maven Shade plugin for executable JAR

### Error Handling
- ✅ Comprehensive error handling in all components
- ✅ Detailed logging of failures and connection issues
- ✅ Failure count tracking and alerting
- ✅ Proper resource cleanup

## Logging

The application uses Log4j2 for logging. Configure logging levels in `src/main/resources/log4j2.xml`.

Default log pattern:
```
yyyy-MM-dd HH:mm:ss LEVEL ClassName - Message
```

## Dependencies

- MySQL Connector/J 8.0.33
- Zendesk MySQL Binlog Connector 0.29.2
- Log4j2 2.20.0
- SLF4J (via Log4j2)

## Troubleshooting

### Connection Issues
- Verify MySQL user has REPLICATION SLAVE and REPLICATION CLIENT privileges
- Check that binary logging is enabled: `SHOW VARIABLES LIKE 'log_bin';`
- Verify network connectivity and firewall settings

### Event Processing
- Check that `binlog_format=ROW` is set in MySQL
- Verify `binlog_row_image=FULL` for complete event data
- Review logs for deserialization errors

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

See LICENSE file for details.

## Future Enhancements

- [ ] Add unit and integration tests
- [ ] Implement table filtering by database/table name
- [ ] Add binlog position checkpointing to external storage
- [ ] Implement retry logic with exponential backoff
- [ ] Add metrics and monitoring support (JMX/Prometheus)
- [ ] Support for multiple output formats (JSON, Avro, Protobuf)
- [ ] Kafka/RabbitMQ integration
- [ ] Docker containerization
- [ ] CI/CD pipeline configuration
- [ ] Database schema introspection for column names
- [ ] Support for DDL event processing
