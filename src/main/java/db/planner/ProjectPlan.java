package db.planner;

import db.query.ProjectScan;
import db.query.Scan;
import db.record.Schema;

import java.util.List;

public class ProjectPlan implements Plan {
    private final Plan plan;
    private final Schema schema;

    public ProjectPlan(Plan plan, List<String> fields) {
        this.schema = new Schema();
        for (String field : fields) {
            schema.add(field, plan.schema());
        }
        this.plan = plan;
    }

    @Override
    public Scan open() {
        Scan scan = plan.open();
        return new ProjectScan(scan, schema.getFields());
    }

    @Override
    public int blockAccessed() {
        return plan.blockAccessed();
    }

    @Override
    public int recordOutput() {
        return plan.recordOutput();
    }

    @Override
    public int distinctValues(String fieldName) {
        return plan.distinctValues(fieldName);
    }

    @Override
    public Schema schema() {
        return schema;
    }
}
