package db.metadata;

import db.record.Layout;
import db.record.Schema;
import db.record.TableScan;
import db.transaction.Transaction;

public class ViewManager {
    private static final int MAX_VIEWDEF = 100;
    private final TableManager tableManager;

    public ViewManager(boolean isNew, TableManager tableManager, Transaction transaction) {
        this.tableManager = tableManager;
        if (isNew) {
            Schema schema = new Schema();
            schema.addStringField("view_name", TableManager.MAX_NAME_LENGTH);
            schema.addStringField("view_def", MAX_VIEWDEF);
            tableManager.createTable("view_metadata", schema, transaction);
        }
    }

    public void createView(String viewName, String viewDef, Transaction transaction) {
        Layout layout = tableManager.getlayout("view_metadata", transaction);
        TableScan tableScan = new TableScan(transaction, "view_metadata", layout);
        tableScan.setString("view_name", viewName);
        tableScan.setString("view_def", viewDef);
        tableScan.close();
    }

    public String getViewDef(String viewName, Transaction transaction) {
        Layout layout = tableManager.getlayout("view_metadata", transaction);
        TableScan tableScan = new TableScan(transaction, "view_metadata", layout);
        while (tableScan.next()) {
            if (tableScan.getString("view_name").equals(viewName)) {
                String result = tableScan.getString("view_def");
                tableScan.close();
                return result;
            }
        }
        return null;
    }
}
