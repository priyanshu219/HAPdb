package db.record;

import db.file.Page;

import java.util.HashMap;
import java.util.Map;

import static java.sql.Types.INTEGER;

public class Layout {
    private final Schema schema;
    private final Map<String, Integer> offsets;
    private final int slotSize;

    public Layout(Schema schema) {
        this.schema = schema;
        this.offsets = new HashMap<>();
        int pos = Integer.BYTES;
        for (String fieldName : schema.getFields()) {
            offsets.put(fieldName, pos);
            pos += lengthInBytes(fieldName);
        }

        this.slotSize = pos;
    }

    public Layout(Schema schema, Map<String, Integer> offsets, int slotSize) {
        this.schema = schema;
        this.offsets = offsets;
        this.slotSize = slotSize;
    }

    public Schema getSchema() {
        return schema;
    }

    public int getFieldOffset(String fieldName) {
        return offsets.get(fieldName);
    }

    public int getSlotSize() {
        return slotSize;
    }

    private int lengthInBytes(String fieldName) {
        int type = schema.getFieldType(fieldName);
        if (type == INTEGER) {
            return Integer.BYTES;
        } else {
            return Page.maxLength(schema.getFieldLength(fieldName));
        }
    }
}
