package parser;

import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import events.Event;

import java.io.Serializable;
import java.util.*;

public class UpdateRowDataParser implements RowDataParser<UpdateRowsEventData> {

    private List<String> columnNames;

    public UpdateRowDataParser(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    @Override
    public Event parse(UpdateRowsEventData eventData, String database, String table) {
        if (eventData == null || eventData.getRows().isEmpty()) {
            return null;
        }

        Event event = new Event();
        event.setDatabase(database);
        event.setTable(table);
        event.setColumnList(columnNames);

        // For UPDATE operations, we have both 'before' and 'after' data
        Map.Entry<Serializable[], Serializable[]> row = eventData.getRows().get(0);

        // Parse 'before' data
        Map<String, Object> changesBefore = new HashMap<>();
        Serializable[] beforeRow = row.getKey();
        for (int i = 0; i < beforeRow.length && i < columnNames.size(); i++) {
            changesBefore.put(columnNames.get(i), beforeRow[i]);
        }

        // Parse 'after' data
        Map<String, Object> changesAfter = new HashMap<>();
        Serializable[] afterRow = row.getValue();
        for (int i = 0; i < afterRow.length && i < columnNames.size(); i++) {
            changesAfter.put(columnNames.get(i), afterRow[i]);
        }

        event.setChangesBefore(changesBefore);
        event.setChangesAfter(changesAfter);

        return event;
    }
}
