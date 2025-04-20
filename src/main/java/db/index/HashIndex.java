package db.index;

import db.query.Constant;
import db.record.Layout;
import db.record.RID;
import db.record.TableScan;
import db.transaction.Transaction;

public class HashIndex implements Index {
    private static final int NUM_BUCKETS = 100;
    private final Transaction transaction;
    private final String indexName;
    private final Layout layout;
    private Constant searchKey;
    private TableScan scan;

    public HashIndex(Transaction transaction, String indexName, Layout layout) {
        this.transaction = transaction;
        this.indexName = indexName;
        this.layout = layout;

        this.searchKey = null;
        this.scan = null;
    }

    public static int searchCost(int numBlocks, int rpd) {
        return numBlocks / HashIndex.NUM_BUCKETS;
    }

    @Override
    public void beforeFirst(Constant searchKey) {
        close();
        this.searchKey = searchKey;
        int bucket = searchKey.hashCode() % NUM_BUCKETS;
        String tableName = indexName + bucket;
        scan = new TableScan(transaction, tableName, layout);
    }

    @Override
    public boolean next() {
        while (scan.next()) {
            if (scan.getValue("dataVal").equals(searchKey)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public RID getDataRid() {
        int blockNum = scan.getInt("block");
        int id = scan.getInt("id");

        return new RID(blockNum, id);
    }

    @Override
    public void insert(Constant dataVal, RID dataRID) {
        beforeFirst(searchKey);
        scan.insert();
        scan.setInt("block", dataRID.blockNum());
        scan.setInt("id", dataRID.blockNum());
        scan.setValue("dataVal", dataVal);
    }

    @Override
    public void delete(Constant dataVal, RID dataRID) {
        beforeFirst(searchKey);
        while (next()) {
            if (getDataRid().equals(dataRID)) {
                scan.delete();
                return;
            }
        }
    }

    @Override
    public void close() {
        if (null != scan) {
            scan.close();
        }
    }
}
