package parser;

import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import events.Event;

import java.io.Serializable;
import java.util.*;

public class InsertDataParser implements RowDataParser<WriteRowsEventData> {

    private List<String> columnNames;

    public InsertDataParser(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    @Override
    public Event parse(WriteRowsEventData eventData, String database, String table) {
        if (eventData == null || eventData.getRows().isEmpty()) {
            return null;
        }

        Event event = new Event();
        event.setDatabase(database);
        event.setTable(table);
        event.setColumnList(columnNames);

        // For INSERT operations, only 'after' data is relevant
        Serializable[] row = eventData.getRows().get(0);
        Map<String, Object> changesAfter = new HashMap<>();

        for (int i = 0; i < row.length && i < columnNames.size(); i++) {
            changesAfter.put(columnNames.get(i), row[i]);
        }

        event.setChangesAfter(changesAfter);
        event.setChangesBefore(null); // No 'before' data for INSERT

        return event;
    }
}
