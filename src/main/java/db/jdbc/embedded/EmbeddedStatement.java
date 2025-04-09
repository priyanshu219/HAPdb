package db.jdbc.embedded;

import db.jdbc.StatementAdapter;
import db.planner.Plan;
import db.planner.Planner;
import db.transaction.Transaction;

import java.sql.ResultSet;
import java.sql.SQLException;

public class EmbeddedStatement extends StatementAdapter {
    private final EmbeddedConnection connection;
    private final Planner planner;

    public EmbeddedStatement(EmbeddedConnection connection, Planner planner) {
        this.connection = connection;
        this.planner = planner;
    }

    @Override
    public ResultSet executeQuery(String query) throws SQLException {
        try {
            Transaction transaction = connection.getTransaction();
            Plan plan = planner.createQuery(query, transaction);
            return new EmbeddedResultSet(plan, connection);
        } catch (RuntimeException ex) {
            connection.rollback();
            throw new SQLException(ex);
        }
    }

    @Override
    public int executeUpdate(String command) throws SQLException {
        try {
            Transaction transaction = connection.getTransaction();
            int result = planner.executeUpdate(command, transaction);
            connection.commit();
            return result;
        } catch (RuntimeException ex) {
            connection.rollback();
            throw new SQLException(ex);
        }
    }
}
