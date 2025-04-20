package db.index.planner;

import db.index.Index;
import db.index.query.IndexJoinScan;
import db.metadata.IndexInfo;
import db.planner.Plan;
import db.query.Scan;
import db.record.Schema;
import db.record.TableScan;

public class IndexJoinPlan implements Plan {
    private final Plan plan1;
    private final Plan plan2;
    private final IndexInfo indexInfo;
    private final String joinField;
    private final Schema schema;

    public IndexJoinPlan(Plan plan1, Plan plan2, IndexInfo indexInfo, String joinField) {
        this.plan1 = plan1;
        this.plan2 = plan2;
        this.indexInfo = indexInfo;
        this.joinField = joinField;
        this.schema = new Schema();

        schema.addAll(plan1.schema());
        schema.addAll(plan2.schema());
    }

    @Override
    public Scan open() {
        Scan scan = plan1.open();
        TableScan tableScan = (TableScan) plan2.open();
        Index index = indexInfo.open();

        return new IndexJoinScan(scan, index, joinField, tableScan);
    }

    @Override
    public int blockAccessed() {
        return plan1.blockAccessed() + (plan1.recordOutput() * indexInfo.blockAccessed()) + recordOutput();
    }

    @Override
    public int recordOutput() {
        return (plan1.recordOutput() * indexInfo.recordsOutput());
    }

    @Override
    public int distinctValues(String fieldName) {
        if (plan1.schema().hasField(fieldName)) {
            return plan1.distinctValues(fieldName);
        } else {
            return plan2.distinctValues(fieldName);
        }
    }

    @Override
    public Schema schema() {
        return schema;
    }
}
