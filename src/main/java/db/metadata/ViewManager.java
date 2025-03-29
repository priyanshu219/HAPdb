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
            schema.addStringField("viewname", TableManager.MAX_NAME_LENGTH);
            schema.addStringField("viewdef", MAX_VIEWDEF);
            tableManager.createTable("viewcat", schema, transaction);
        }
    }

    public void createView(String viewName, String viewDef, Transaction transaction) {
        Layout layout = tableManager.getlayout("viewcat", transaction);
        TableScan tableScan = new TableScan(transaction, "viewcat", layout);
        tableScan.setString("viewname", viewName);
        tableScan.setString("viewdef", viewDef);
        tableScan.close();
    }

    public String getViewDef(String vName, Transaction transaction) {
        Layout layout = tableManager.getlayout("viewcat", transaction);
        TableScan tableScan = new TableScan(transaction, "viewcat", layout);
        while (tableScan.next()) {
            if (tableScan.getString("viewName").equals(vName)) {
                String result = tableScan.getString("viewdef");
                tableScan.close();
                return result;
            }
        }
        return null;
    }
}
