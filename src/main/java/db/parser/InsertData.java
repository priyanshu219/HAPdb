package db.parser;

import db.query.Constant;

import java.util.List;

public record InsertData(String tableName, List<String> fields, List<Constant> values) {
}
