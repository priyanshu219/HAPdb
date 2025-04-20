package db.index.query;

import db.index.Index;
import db.query.Constant;
import db.query.Scan;
import db.record.RID;
import db.record.TableScan;

public class IndexSelectScan implements Scan {
    private final TableScan scan;
    private final Index index;
    private final Constant value;

    public IndexSelectScan(TableScan scan, Index index, Constant value) {
        this.scan = scan;
        this.index = index;
        this.value = value;
        beforeFirst();
    }

    @Override
    public void beforeFirst() {
        index.beforeFirst(value);
    }

    @Override
    public boolean next() {
        boolean nextValuePresent = index.next();
        if (nextValuePresent) {
            RID rid = index.getDataRid();
            scan.moveToRID(rid);
        }
        return nextValuePresent;
    }

    @Override
    public int getInt(String fieldName) {
        return scan.getInt(fieldName);
    }

    @Override
    public String getString(String fieldName) {
        return scan.getString(fieldName);
    }

    @Override
    public Constant getValue(String fieldName) {
        return scan.getValue(fieldName);
    }

    @Override
    public boolean hasField(String fieldName) {
        return scan.hasField(fieldName);
    }

    @Override
    public void close() {
        index.close();
        scan.close();
    }
}
