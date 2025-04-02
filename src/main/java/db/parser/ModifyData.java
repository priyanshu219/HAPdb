package db.parser;

import db.query.Expression;
import db.query.Predicate;

public record ModifyData(String tableName, String fieldName, Expression newValue, Predicate predicate) {
    public String targetField() {
        return fieldName;
    }
}
