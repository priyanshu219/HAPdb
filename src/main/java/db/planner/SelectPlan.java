package db.planner;

import db.query.Predicate;
import db.query.Scan;
import db.query.SelectScan;
import db.record.Schema;

public class SelectPlan implements Plan {
    private final Plan plan;
    private final Predicate predicate;

    public SelectPlan(Plan plan, Predicate predicate) {
        this.plan = plan;
        this.predicate = predicate;
    }

    @Override
    public Scan open() {
        Scan scan = plan.open();
        return new SelectScan(scan, predicate);
    }

    @Override
    public int blockAccessed() {
        return plan.blockAccessed();
    }

    @Override
    public int recordOutput() {
        return plan.recordOutput() / predicate.reductionFactor(plan);
    }

    @Override
    public int distinctValues(String fieldName) {
        if (null != predicate.equatesWithConstant(fieldName)) {
            return 1;
        } else {
            String fieldName1 = predicate.equatesWithField(fieldName);
            if (null != fieldName1) {
                return Math.min(plan.distinctValues(fieldName1), plan.distinctValues(fieldName));
            } else {
                return plan.distinctValues(fieldName);
            }
        }
    }

    @Override
    public Schema schema() {
        return null;
    }
}
