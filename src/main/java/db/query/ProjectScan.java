package db.query;

import java.util.List;

public class ProjectScan implements Scan {
    private final Scan scan;
    private final List<String> fields;

    public ProjectScan(Scan scan, List<String> fields) {
        this.scan = scan;
        this.fields = fields;
    }

    @Override
    public void beforeFirst() {
        scan.beforeFirst();
    }

    @Override
    public boolean next() {
        return scan.next();
    }

    @Override
    public int getInt(String fieldName) {
        if (hasField(fieldName)) {
            return scan.getInt(fieldName);
        } else {
            throw new RuntimeException("field not found");
        }
    }

    @Override
    public String getString(String fieldName) {
        if (hasField(fieldName)) {
            return scan.getString(fieldName);
        } else {
            throw new RuntimeException("field not found");
        }
    }

    @Override
    public Constant getValue(String fieldName) {
        if (hasField(fieldName)) {
            return scan.getValue(fieldName);
        } else {
            throw new RuntimeException("field not found");
        }
    }

    @Override
    public boolean hasField(String fieldName) {
        return fields.contains(fieldName);
    }

    @Override
    public void close() {
        scan.close();
    }
}
