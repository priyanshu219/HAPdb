package db.jdbc.embedded;

import db.jdbc.ResultSetAdapter;
import db.planner.Plan;
import db.query.Scan;
import db.record.Schema;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class EmbeddedResultSet extends ResultSetAdapter {
    private final Scan scan;
    private final Schema schema;
    private final EmbeddedConnection connection;

    public EmbeddedResultSet(Plan plan, EmbeddedConnection connection) {
        this.scan = plan.open();
        this.schema = plan.schema();
        this.connection = connection;
    }

    @Override
    public boolean next() throws SQLException {
        try {
            return scan.next();
        } catch (RuntimeException ex) {
            connection.rollback();
            throw new SQLException(ex);
        }
    }

    @Override
    public int getInt(String fieldName) throws SQLException {
        try {
            fieldName = fieldName.toLowerCase();
            return scan.getInt(fieldName);
        } catch (RuntimeException ex) {
            connection.rollback();
            throw new SQLException(ex);
        }
    }

    @Override
    public String getString(String fieldName) throws SQLException {
        try {
            fieldName = fieldName.toLowerCase();
            return scan.getString(fieldName);
        } catch (RuntimeException ex) {
            connection.rollback();
            throw new SQLException(ex);
        }
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new EmbeddedMetaData(schema);
    }

    @Override
    public void close() throws SQLException {
        scan.close();
        connection.commit();
    }
}
