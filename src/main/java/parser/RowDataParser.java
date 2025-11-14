package parser;

import com.github.shyiko.mysql.binlog.event.EventData;
import events.Event;

public interface RowDataParser<T extends EventData> {
    Event parse(T eventData, String database, String table);
}
