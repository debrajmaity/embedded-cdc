package parser;

import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import events.Event;

import java.io.Serializable;
import java.util.*;

public class DeleteDataParser implements RowDataParser<DeleteRowsEventData> {

    private List<String> columnNames;

    public DeleteDataParser(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    @Override
    public Event parse(DeleteRowsEventData eventData, String database, String table) {
        if (eventData == null || eventData.getRows().isEmpty()) {
            return null;
        }

        Event event = new Event();
        event.setDatabase(database);
        event.setTable(table);
        event.setColumnList(columnNames);

        // For DELETE operations, only 'before' data is relevant
        Serializable[] row = eventData.getRows().get(0);
        Map<String, Object> changesBefore = new HashMap<>();

        for (int i = 0; i < row.length && i < columnNames.size(); i++) {
            changesBefore.put(columnNames.get(i), row[i]);
        }

        event.setChangesBefore(changesBefore);
        event.setChangesAfter(null); // No 'after' data for DELETE

        return event;
    }
}
