package db.query;

import db.record.RID;

public class SelectScan implements UpdateScan {
    private final Scan scan;
    private final Predicate predicate;

    public SelectScan(Scan scan, Predicate predicate) {
        this.scan = scan;
        this.predicate = predicate;
    }

    @Override
    public void setInt(String fieldName, int value) {
        UpdateScan updateScan = (UpdateScan) scan;
        updateScan.setInt(fieldName, value);
    }

    @Override
    public void setString(String fieldName, String value) {
        UpdateScan updateScan = (UpdateScan) scan;
        updateScan.setString(fieldName, value);
    }

    @Override
    public void setValue(String fieldName, Constant value) {
        UpdateScan updateScan = (UpdateScan) scan;
        updateScan.setValue(fieldName, value);
    }

    @Override
    public void insert() {
        UpdateScan updateScan = (UpdateScan) scan;
        updateScan.insert();
    }

    @Override
    public void delete() {
        UpdateScan updateScan = (UpdateScan) scan;
        updateScan.delete();
    }

    @Override
    public RID getRID() {
        UpdateScan updateScan = (UpdateScan) scan;
        return updateScan.getRID();
    }

    @Override
    public void moveToRID(RID rid) {
        UpdateScan updateScan = (UpdateScan) scan;
        updateScan.moveToRID(rid);
    }

    @Override
    public void beforeFirst() {
        scan.beforeFirst();
    }

    @Override
    public boolean next() {
        while (scan.next()) {
            if (predicate.isSatisfied(scan)) {
                return true;
            }
        }
        return false;
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

    }
}
