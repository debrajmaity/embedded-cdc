package events;

public enum EventType {
    UPDATE("update"),
    INSERT("insert"),
    DELETE("delete");

    private String value;

    EventType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
