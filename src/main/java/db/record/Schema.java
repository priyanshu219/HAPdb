package db.record;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;

public class Schema {
    private final List<String> fields;
    private final Map<String, FieldInfo> infoMap;

    public Schema() {
        this.fields = new ArrayList<>();
        this.infoMap = new HashMap<>();
    }

    public void addField(String fieldName, int type, int length) {
        fields.add(fieldName);
        infoMap.put(fieldName, new FieldInfo(type, length));
    }

    public void addIntField(String fieldName) {
        addField(fieldName, INTEGER, 0);
    }

    public void addStringField(String fieldName, int length) {
        addField(fieldName, VARCHAR, length);
    }

    public void addAll(Schema schema) {
        for (String fieldName : schema.getFields()) {
            add(fieldName, schema);
        }
    }

    public void add(String fieldName, Schema schema) {
        int type = schema.getFieldType(fieldName);
        int length = schema.getFieldLength(fieldName);
        addField(fieldName, type, length);
    }

    public List<String> getFields() {
        return fields;
    }

    public int getFieldType(String fieldName) {
        return infoMap.get(fieldName).type;
    }

    public int getFieldLength(String fieldName) {
        return infoMap.get(fieldName).length;
    }

    public boolean hasField(String fieldName) {
        return (null != infoMap.get(fieldName));
    }

    private record FieldInfo(int type, int length) {
    }
}
