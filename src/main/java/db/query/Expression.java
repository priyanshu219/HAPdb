package db.query;

import db.record.Schema;

public class Expression {
    private final Constant val;
    private final String fieldName;

    public Expression(Constant val) {
        this.val = val;
        this.fieldName = null;
    }

    public Expression(String fieldName) {
        this.val = null;
        this.fieldName = fieldName;
    }

    public boolean isField() {
        return (null != fieldName);
    }

    public Constant asConstant() {
        return val;
    }

    public String asFieldName() {
        return fieldName;
    }

    public Constant evaluate(Scan scan) {
        return (null != val) ? val : scan.getValue(fieldName);
    }

    public boolean appliesTo(Schema schema) {
        return null != val || schema.hasField(fieldName);
    }

    @Override
    public String toString() {
        return (null != val) ? val.toString() : fieldName;
    }
}
