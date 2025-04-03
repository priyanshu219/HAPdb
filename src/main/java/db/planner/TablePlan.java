package db.planner;

import db.metadata.MetadataManager;
import db.metadata.StatInfo;
import db.query.Scan;
import db.record.Layout;
import db.record.Schema;
import db.record.TableScan;
import db.transaction.Transaction;

public class TablePlan implements Plan {
    public final StatInfo statInfo;
    private final Transaction transaction;
    private final String tableName;
    private final Layout layout;

    public TablePlan(Transaction transaction, String tableName, MetadataManager metadataManager) {
        this.transaction = transaction;
        this.tableName = tableName;
        this.layout = metadataManager.getLayout(tableName, transaction);
        this.statInfo = metadataManager.getStatInfo(tableName, layout, transaction);
    }


    @Override
    public Scan open() {
        return new TableScan(transaction, tableName, layout);
    }

    @Override
    public int blockAccessed() {
        return statInfo.blocksAccessed();
    }

    @Override
    public int recordOutput() {
        return statInfo.recordsOutput();
    }

    @Override
    public int distinctValues(String fieldName) {
        return statInfo.getDistinctValues(fieldName);
    }

    @Override
    public Schema schema() {
        return layout.getSchema();
    }
}
