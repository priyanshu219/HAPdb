package db.query;

import db.planner.Plan;
import db.record.Schema;

public class Term {
    private final Expression lhs, rhs;

    public Term(Expression lhs, Expression rhs) {
        this.lhs = lhs;
        this.rhs = rhs;
    }

    public boolean isSatisfied(Scan scan) {
        Constant lhsValue = lhs.evaluate(scan);
        Constant rhsValue = rhs.evaluate(scan);

        return lhsValue.equals(rhsValue);
    }

    public boolean appliesTo(Schema schema) {
        return lhs.appliesTo(schema) && rhs.appliesTo(schema);
    }

    public int reductionFactor(Plan plan) {
        String lhsName;
        String rhsName;

        if (lhs.isField() && rhs.isField()) {
            lhsName = lhs.asFieldName();
            rhsName = rhs.asFieldName();
            return Math.max(plan.distinctValues(lhsName), plan.distinctValues(rhsName));
        }
        if (lhs.isField()) {
            lhsName = lhs.asFieldName();
            return plan.distinctValues(lhsName);
        }
        if (rhs.isField()) {
            rhsName = rhs.asFieldName();
            return plan.distinctValues(rhsName);
        }

        if (lhs.asConstant().equals(rhs.asConstant())) {
            return 1;
        } else {
            return Integer.MAX_VALUE;
        }
    }

    public Constant equatesWithConstant(String fieldName) {
        if (lhs.isField() && lhs.asFieldName().equals(fieldName) && !rhs.isField()) {
            return rhs.asConstant();
        } else if (rhs.isField() && rhs.asFieldName().equals(fieldName) && !lhs.isField()) {
            return lhs.asConstant();
        } else {
            return null;
        }
    }

    public String equatesWithField(String fieldName) {
        if (lhs.isField() && lhs.asFieldName().equals(fieldName) && rhs.isField()) {
            return rhs.asFieldName();
        } else if (rhs.isField() && rhs.asFieldName().equals(fieldName) && lhs.isField()) {
            return lhs.asFieldName();
        } else {
            return null;
        }
    }

    @Override
    public String toString() {
        return lhs.toString() + " = " + rhs.toString();
    }
}
