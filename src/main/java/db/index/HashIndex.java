package db.index;

import db.query.Constant;
import db.record.Layout;
import db.record.RID;
import db.transaction.Transaction;

public class HashIndex implements Index {
    public HashIndex(Transaction transaction, String indexName, Layout indexLayout) {
    }

    public static int searchCost(int numBlocks, int rpd) {
        return 0;
    }

    @Override
    public void beforeFirst(Constant searchKey) {

    }

    @Override
    public boolean next() {
        return false;
    }

    @Override
    public RID getDataRid() {
        return null;
    }

    @Override
    public void insert(Constant dataVal, RID dataRID) {

    }

    @Override
    public void delete(Constant dataVal, RID dataRID) {

    }

    @Override
    public void close() {

    }
}
