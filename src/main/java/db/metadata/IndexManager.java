package db.metadata;

import db.record.Layout;
import db.record.Schema;
import db.record.TableScan;
import db.transaction.Transaction;

import java.util.HashMap;
import java.util.Map;

public class IndexManager {
    private static final int MAX_NAME = 1000;
    private final Layout layout;
    private final TableManager tableManager;
    private final StatManager statManager;

    public IndexManager(boolean isNew, TableManager tableManager, StatManager statManager, Transaction transaction) {
        if (isNew) {
            Schema schema = new Schema();
            schema.addStringField("index_name", MAX_NAME);
            schema.addStringField("table_name", MAX_NAME);
            schema.addStringField("field_name", MAX_NAME);
            tableManager.createTable("index_metadata", schema, transaction);
        }
        this.tableManager = tableManager;
        this.statManager = statManager;
        this.layout = tableManager.getlayout("index_metadata", transaction);
    }

    public void createIndex(String indexName, String tableName, String fieldName, Transaction transaction) {
        TableScan scan = new TableScan(transaction, tableName, layout);
        scan.insert();
        scan.setString("index_name", indexName);
        scan.setString("table_name", tableName);
        scan.setString("field_name", fieldName);
        scan.close();
    }

    public Map<String, IndexInfo> getIndexInfo(String tableName, Transaction transaction) {
        Map<String, IndexInfo> indexInfoMap = new HashMap<>();
        TableScan scan = new TableScan(transaction, "index_metadata", layout);
        while (scan.next()) {
            if (scan.getString("table_name").equals(tableName)) {
                String indexName = scan.getString("index_name");
                String fieldName = scan.getString("field_name");
                Layout tableLayout = tableManager.getlayout(tableName, transaction);
                StatInfo statInfo = statManager.getStateInfo(tableName, tableLayout, transaction);
                IndexInfo indexInfo = new IndexInfo(indexName, fieldName, tableLayout, transaction, statInfo);
                indexInfoMap.put(fieldName, indexInfo);
            }
        }

        scan.close();
        return indexInfoMap;
    }
}
