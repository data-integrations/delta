package io.cdap.delta.api;

import io.cdap.cdap.api.data.schema.Schema;

/**
 * Used to store type and value of a sort key
 * List of sort keys is used for ordering DML events
 */
public class SortKey {
    private final Schema.Type type;
    private final Object value;

    public SortKey(Schema.Type type, Object value) {
        this.type = type;
        this.value = value;
    }

    public Schema.Type getType() {
        return type;
    }

    public <T> T getValue() {
        return (T) value;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SortKey{");
        sb.append("type=").append(type);
        sb.append(", value=").append(value);
        sb.append('}');
        return sb.toString();
    }
}
