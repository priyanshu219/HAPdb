package db.planner;

import db.query.ProductScan;
import db.query.Scan;
import db.record.Schema;

public class ProductPlan implements Plan {
    private final Plan plan1, plan2;
    private final Schema schema;

    public ProductPlan(Plan plan1, Plan plan2) {
        this.plan1 = plan1;
        this.plan2 = plan2;

        this.schema = new Schema();
        schema.addAll(plan1.schema());
        schema.addAll(plan2.schema());
    }

    @Override
    public Scan open() {
        Scan scan1 = plan1.open();
        Scan scan2 = plan2.open();
        return new ProductScan(scan1, scan2);
    }

    @Override
    public int blockAccessed() {
        return (plan1.blockAccessed() + (plan1.recordOutput() * plan2.blockAccessed()));
    }

    @Override
    public int recordOutput() {
        return (plan1.recordOutput() + plan2.recordOutput());
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
