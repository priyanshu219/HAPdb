package db.index.planner;

import db.index.Index;
import db.index.query.IndexSelectScan;
import db.metadata.IndexInfo;
import db.planner.Plan;
import db.query.Constant;
import db.query.Scan;
import db.record.Schema;
import db.record.TableScan;

public class IndexSelectPlan implements Plan {
    private final Plan plan;
    private final IndexInfo indexInfo;
    private final Constant value;

    public IndexSelectPlan(Plan plan, IndexInfo indexInfo, Constant value) {
        this.plan = plan;
        this.indexInfo = indexInfo;
        this.value = value;
    }

    @Override
    public Scan open() {
        TableScan scan = (TableScan) plan.open();
        Index index = indexInfo.open();
        return new IndexSelectScan(scan, index, value);
    }

    @Override
    public int blockAccessed() {
        return indexInfo.blockAccessed() + recordOutput();
    }

    @Override
    public int recordOutput() {
        return indexInfo.recordsOutput();
    }

    @Override
    public int distinctValues(String fieldName) {
        return indexInfo.distinctValues(fieldName);
    }

    @Override
    public Schema schema() {
        return plan.schema();
    }
}
