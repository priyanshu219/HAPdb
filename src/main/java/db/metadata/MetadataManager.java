package db.metadata;

import db.record.Layout;
import db.record.Schema;
import db.transaction.Transaction;

public class MetadataManager {
    private final TableManager tableManager;
    private final ViewManager viewManager;
    private final StatManager statManager;

    public MetadataManager(boolean isNew, Transaction transaction) {
        tableManager = new TableManager(isNew, transaction);
        viewManager = new ViewManager(isNew, tableManager, transaction);
        statManager = new StatManager(tableManager, transaction);
    }

    public void createTable(String tableName, Schema schema, Transaction transaction) {
        tableManager.createTable(tableName, schema, transaction);
    }

    public Layout getLayout(String tableName, Transaction transaction) {
        return tableManager.getlayout(tableName, transaction);
    }

    public void createView(String viewName, String viewDef, Transaction transaction) {
        viewManager.createView(viewName, viewDef, transaction);
    }

    public String getViewDef(String viewName, Transaction transaction) {
        return viewManager.getViewDef(viewName, transaction);
    }

    public StatInfo getStatInfo(String tableName, Layout layout, Transaction transaction) {
        return statManager.getStateInfo(tableName, layout, transaction);
    }
}
