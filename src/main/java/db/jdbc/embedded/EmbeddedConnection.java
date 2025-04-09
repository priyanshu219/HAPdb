package db.jdbc.embedded;

import db.jdbc.ConnectionAdapter;
import db.planner.Planner;
import db.server.HAPdb;
import db.transaction.Transaction;

import java.sql.SQLException;
import java.sql.Statement;

public class EmbeddedConnection extends ConnectionAdapter {
    private final HAPdb db;
    private final Planner planner;
    private Transaction transaction;

    public EmbeddedConnection(HAPdb db) {
        this.db = db;
        this.transaction = db.getNewTransaction();
        this.planner = db.getPlanner();
    }

    @Override
    public Statement createStatement() throws SQLException {
        return new EmbeddedStatement(this, planner);
    }

    @Override
    public void close() throws SQLException {
        commit();
    }

    @Override
    public void commit() throws SQLException {
        transaction.commit();
        transaction = db.getNewTransaction();
    }

    @Override
    public void rollback() throws SQLException {
        transaction.commit();
        transaction = db.getNewTransaction();
    }

    Transaction getTransaction() {
        return transaction;
    }
}
