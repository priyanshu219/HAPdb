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

    public synchronized StatInfo getStateInfo(String tblName, Layout layout, Transaction transaction) {
        numCalls++;
        if (numCalls >= 100) {
            numCalls = 0;
            refreshStatics(transaction);
        }
        StatInfo statInfo = tableStats.get(tblName);
        if (null == statInfo) {
            statInfo = calcTableStats(tblName, layout, transaction);
            tableStats.put(tblName, statInfo);
        }
        return statInfo;
    }

    private synchronized void refreshStatics(Transaction transaction) {
        tableStats = new HashMap<>();
        numCalls = 0;
        Layout tcatLayout = tableManager.getlayout("tblcat", transaction);
        TableScan tableScan = new TableScan(transaction, "tblcat", tcatLayout);
        while (tableScan.next()) {
            String tblName = tableScan.getString("tblname");
            Layout layout = tableManager.getlayout(tblName, transaction);
            StatInfo statInfo = calcTableStats(tblName, layout, transaction);
            tableStats.put(tblName, statInfo);
        }
        tableScan.close();
    }

    private synchronized StatInfo calcTableStats(String tblName, Layout layout, Transaction transaction) {
        TableScan tableScan = new TableScan(transaction, tblName, layout);
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
