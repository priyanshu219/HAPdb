package db.jdbc.embedded;

import db.jdbc.ResultSetMetaDataAdapter;
import db.record.Schema;

import static java.sql.Types.INTEGER;

public class EmbeddedMetaData extends ResultSetMetaDataAdapter {
    private final Schema schema;

    public EmbeddedMetaData(Schema schema) {
        this.schema = schema;
    }

    @Override
    public int getColumnCount() {
        return schema.getFields().size();
    }

    @Override
    public String getColumnName(int column) {
        return schema.getFields().get(column - 1);
    }

    @Override
    public int getColumnType(int column) {
        String fieldName = getColumnName(column);
        return schema.getFieldType(fieldName);
    }

    @Override
    public int getColumnDisplaySize(int column) {
        String fieldName = getColumnName(column);
        int fieldType = schema.getFieldType(fieldName);
        int fieldLength = (fieldType == INTEGER) ? 6 : schema.getFieldLength(fieldName);
        return Math.max(fieldName.length(), fieldLength);
    }
}
