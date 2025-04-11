package db.metadata;

import db.record.Layout;
import db.record.Schema;
import db.record.TableScan;
import db.transaction.Transaction;

import java.util.HashMap;
import java.util.Map;

public class TableManager {
    public static final int MAX_NAME_LENGTH = 16;
    private final Layout tableMetadataLayout;
    private final Layout fieldMetadataLayout;

    public TableManager(boolean isNew, Transaction transaction) {
        Schema tableMetadataSchema = new Schema();
        tableMetadataSchema.addStringField("table_name", MAX_NAME_LENGTH);
        tableMetadataSchema.addIntField("slot_size");
        tableMetadataLayout = new Layout(tableMetadataSchema);

        Schema fieldMetadataSchema = new Schema();
        fieldMetadataSchema.addStringField("table_name", MAX_NAME_LENGTH);
        fieldMetadataSchema.addStringField("field_name", MAX_NAME_LENGTH);
        fieldMetadataSchema.addIntField("type");
        fieldMetadataSchema.addIntField("length");
        fieldMetadataSchema.addIntField("offset");
        fieldMetadataLayout = new Layout(fieldMetadataSchema);

        if (isNew) {
            createTable("table_metadata", tableMetadataSchema, transaction);
            createTable("field_metadata", fieldMetadataSchema, transaction);
        }
    }

    public void createTable(String tableName, Schema schema, Transaction transaction) {
        Layout layout = new Layout(schema);

        TableScan tcat = new TableScan(transaction, "table_metadata", tableMetadataLayout);
        tcat.insert();
        tcat.setString("table_name", tableName);
        tcat.setInt("slot_size", layout.getSlotSize());
        tcat.close();

        TableScan fcat = new TableScan(transaction, "field_metadata", fieldMetadataLayout);
        fcat.insert();

        for (String fieldName : schema.getFields()) {
            fcat.setString("table_name", tableName);
            fcat.setString("field_name", fieldName);
            fcat.setInt("type", schema.getFieldType(fieldName));
            fcat.setInt("length", schema.getFieldLength(fieldName));
            fcat.setInt("offset", layout.getFieldOffset(fieldName));
        }

        fcat.close();
    }

    public Layout getlayout(String tableName, Transaction transaction) {
        int size = -1;
        TableScan tableMetadataScan = new TableScan(transaction, "table_metadata", tableMetadataLayout);
        while (tableMetadataScan.next()) {
            if (tableMetadataScan.getString("table_name").equals(tableName)) {
                size = tableMetadataScan.getInt("slot_size");
                break;
            }
        }
        tableMetadataScan.close();

        Schema schema = new Schema();
        TableScan fieldMetadataScan = new TableScan(transaction, "field_metadata", fieldMetadataLayout);
        Map<String, Integer> offsets = new HashMap<>();
        while (fieldMetadataScan.next()) {
            if (fieldMetadataScan.getString("table_name").equals(tableName)) {
                String fieldName = fieldMetadataScan.getString("field_name");
                int fieldType = fieldMetadataScan.getInt("type");
                int fieldLength = fieldMetadataScan.getInt("length");
                int fieldOffset = fieldMetadataScan.getInt("offset");
                offsets.put(fieldName, fieldOffset);
                schema.addField(fieldName, fieldType, fieldLength);
            }
        }
        fieldMetadataScan.close();
        return new Layout(schema, offsets, size);
    }
}
