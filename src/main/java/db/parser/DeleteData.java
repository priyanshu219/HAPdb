package db.parser;

import db.query.Predicate;

public record DeleteData(String tableName, Predicate predicate) {
}
