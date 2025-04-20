package db.index.query;

import db.index.Index;
import db.query.Constant;
import db.query.Scan;
import db.record.TableScan;

public class IndexJoinScan implements Scan {
    private final Scan lhsScan; // 'scan' is scan of table1 where A is the column name in that
    private final Index index;
    private final String joinField;
    private final TableScan rhsScan;

    public IndexJoinScan(Scan lhsScan, Index index, String joinField, TableScan rhsScan) {
        this.lhsScan = lhsScan;
        this.index = index;
        this.joinField = joinField;
        this.rhsScan = rhsScan;
        beforeFirst();
    }

    @Override
    public void beforeFirst() {
        lhsScan.beforeFirst();
        lhsScan.next();
        resetIndex();
    }

    @Override
    public boolean next() {
        while (true) {
            if (index.next()) {
                rhsScan.moveToRID(index.getDataRid());
                return true;
            }
            if (!lhsScan.next()) {
                return false;
            }
            resetIndex();
        }
    }

    @Override
    public int getInt(String fieldName) {
        if (rhsScan.hasField(fieldName)) {
            return rhsScan.getInt(fieldName);
        } else {
            return lhsScan.getInt(fieldName);
        }
    }

    @Override
    public String getString(String fieldName) {
        if (rhsScan.hasField(fieldName)) {
            return rhsScan.getString(fieldName);
        } else {
            return lhsScan.getString(fieldName);
        }
    }

    @Override
    public Constant getValue(String fieldName) {
        if (rhsScan.hasField(fieldName)) {
            return rhsScan.getValue(fieldName);
        } else {
            return lhsScan.getValue(fieldName);
        }
    }

    @Override
    public boolean hasField(String fieldName) {
        return (rhsScan.hasField(fieldName) || lhsScan.hasField(fieldName));
    }

    @Override
    public void close() {
        rhsScan.close();
        lhsScan.close();
        index.close();
    }

    private void resetIndex() {
        Constant searchKey = lhsScan.getValue(joinField);
        index.beforeFirst(searchKey);
    }
}
