package db.metadata;

import db.record.Layout;
import db.record.Schema;
import db.record.TableScan;
import db.transaction.Transaction;

import java.util.HashMap;
import java.util.Map;

public class TableManager {
    public static final int MAX_NAME_LENGTH = 16;
    private final Layout tcatLayout;
    private final Layout fcatLayout;

    public TableManager(boolean isNew, Transaction transaction) {
        Schema tcatSchema = new Schema();
        tcatSchema.addStringField("tblname", MAX_NAME_LENGTH);
        tcatSchema.addIntField("slotsize");
        tcatLayout = new Layout(tcatSchema);

        Schema fcatSchema = new Schema();
        fcatSchema.addStringField("tblname", MAX_NAME_LENGTH);
        fcatSchema.addStringField("fldname", MAX_NAME_LENGTH);
        fcatSchema.addIntField("type");
        fcatSchema.addIntField("length");
        fcatSchema.addIntField("offset");
        fcatLayout = new Layout(fcatSchema);
    }

    public void createTable(String tblName, Schema schema, Transaction transaction) {
        Layout layout = new Layout(schema);

        TableScan tcat = new TableScan(transaction, "tblcat", tcatLayout);
        tcat.insert();
        tcat.setString("tblname", tblName);
        tcat.setInt("slotsize", layout.getSlotSize());
        tcat.close();

        TableScan fcat = new TableScan(transaction, "fldcat", fcatLayout);
        fcat.insert();

        for (String fieldName : schema.getFields()) {
            fcat.setString("tblname", tblName);
            fcat.setString("fldname", fieldName);
            fcat.setInt("type", schema.getFieldType(fieldName));
            fcat.setInt("length", schema.getFieldLength(fieldName));
            fcat.setInt("offset", layout.getFieldOffset(fieldName));
        }

        fcat.close();
    }

    public Layout getlayout(String tblName, Transaction transaction) {
        int size = -1;
        TableScan tcatScan = new TableScan(transaction, "tblcat", tcatLayout);
        while (tcatScan.next()) {
            if (tcatScan.getString("tblname").equals(tblName)) {
                size = tcatScan.getInt("slotsize");
                break;
            }
        }
        tcatScan.close();

        Schema schema = new Schema();
        TableScan fldcatScan = new TableScan(transaction, "fldcat", fcatLayout);
        Map<String, Integer> offsets = new HashMap<>();
        while (fldcatScan.next()) {
            if (fldcatScan.getString("tblname").equals(tblName)) {
                String fldName = fldcatScan.getString("fldname");
                int fldType = fldcatScan.getInt("type");
                int fldLength = fldcatScan.getInt("length");
                int fldOffset = fldcatScan.getInt("offset");
                offsets.put(fldName, fldOffset);
                schema.addField(fldName, fldType, fldLength);
            }
        }
        fldcatScan.close();
        return new Layout(schema, offsets, size);
    }
}
