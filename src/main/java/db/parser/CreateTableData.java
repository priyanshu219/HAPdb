package db.parser;

import db.record.Schema;

public record CreateTableData(String tableName, Schema schema) {
    public Schema newSchema() {
        return schema;
    }
}
