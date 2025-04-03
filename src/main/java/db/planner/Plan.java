package db.planner;

import db.query.Scan;
import db.record.Schema;

public interface Plan {
    Scan open();

    int blockAccessed();

    int recordOutput();

    int distinctValues(String fieldName);

    Schema schema();
}
