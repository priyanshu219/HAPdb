package db.metadata;

import db.record.Layout;
import db.record.TableScan;
import db.transaction.Transaction;

import java.util.HashMap;
import java.util.Map;

public class StatManager {
    private final TableManager tableManager;
    private Map<String, StatInfo> tableStats;
    private int numCalls;

    public StatManager(TableManager tableManager, Transaction transaction) {
        this.tableManager = tableManager;
        refreshStatics(transaction);
    }

    public synchronized StatInfo getStateInfo(String tableName, Layout layout, Transaction transaction) {
        numCalls++;
        if (numCalls >= 100) {
            numCalls = 0;
            refreshStatics(transaction);
        }
        StatInfo statInfo = tableStats.get(tableName);
        if (null == statInfo) {
            statInfo = calcTableStats(tableName, layout, transaction);
            tableStats.put(tableName, statInfo);
        }
        return statInfo;
    }

    private synchronized void refreshStatics(Transaction transaction) {
        tableStats = new HashMap<>();
        numCalls = 0;
        Layout tableMetadataLayout = tableManager.getlayout("table_metadata", transaction);
        TableScan tableScan = new TableScan(transaction, "table_metadata", tableMetadataLayout);
        while (tableScan.next()) {
            String tableName = tableScan.getString("table_name");
            Layout layout = tableManager.getlayout(tableName, transaction);
            StatInfo statInfo = calcTableStats(tableName, layout, transaction);
            tableStats.put(tableName, statInfo);
        }
        tableScan.close();
    }

    private synchronized StatInfo calcTableStats(String tableName, Layout layout, Transaction transaction) {
        TableScan tableScan = new TableScan(transaction, tableName, layout);
        int totalRecords = 0;
        int totalBlocks = 0;

        while (tableScan.next()) {
            totalRecords++;
            totalBlocks = tableScan.getRID().blockNum() + 1;
        }

        tableScan.close();
        return new StatInfo(totalBlocks, totalRecords);
    }
}
