package db.query;

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

    @Override
    public String toString() {
        return lhs.toString() + " = " + rhs.toString();
    }
}
