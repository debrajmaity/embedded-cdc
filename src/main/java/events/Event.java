package events;

import java.util.List;
import java.util.Map;

public class Event {
    public String database;
    public String table;
    public List<String> columnList;
    public Map<String, Object> changesBefore;
    public Map<String, Object> changesAfter;

    public List<String> getColumnList() {
        return columnList;
    }

    public void setColumnList(List<String> columnList) {
        this.columnList = columnList;
    }

    public Map<String, Object> getChangesBefore() {
        return changesBefore;
    }

    public void setChangesBefore(Map<String, Object> changesBefore) {
        this.changesBefore = changesBefore;
    }

    public Map<String, Object> getChangesAfter() {
        return changesAfter;
    }

    public void setChangesAfter(Map<String, Object> changesAfter) {
        this.changesAfter = changesAfter;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }
}
