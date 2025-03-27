package db.query;

public class ProductScan implements Scan{
    private final Scan lhsScan, rhsScan;

    public ProductScan(Scan lhsScan, Scan rhsScan) {
        this.lhsScan = lhsScan;
        this.rhsScan = rhsScan;
    }

    @Override
    public void beforeFirst() {
        lhsScan.beforeFirst();
        lhsScan.next();
        rhsScan.beforeFirst();
    }

    @Override
    public boolean next() {
        if (rhsScan.next()) {
            return true;
        } else {
            rhsScan.beforeFirst();
            return (lhsScan.next() && rhsScan.next());
        }
    }

    @Override
    public int getInt(String fieldName) {
        if (lhsScan.hasField(fieldName)) {
            return lhsScan.getInt(fieldName);
        } else {
            return rhsScan.getInt(fieldName);
        }
    }

    @Override
    public String getString(String fieldName) {
        if (lhsScan.hasField(fieldName)) {
            return lhsScan.getString(fieldName);
        } else {
            return rhsScan.getString(fieldName);
        }
    }

    @Override
    public Constant getValue(String fieldName) {
        if (lhsScan.hasField(fieldName)) {
            return lhsScan.getValue(fieldName);
        } else {
            return rhsScan.getValue(fieldName);
        }
    }

    @Override
    public boolean hasField(String fieldName) {
        return (lhsScan.hasField(fieldName) || rhsScan.hasField(fieldName));
    }

    @Override
    public void close() {
        lhsScan.close();
        rhsScan.close();
    }
}
