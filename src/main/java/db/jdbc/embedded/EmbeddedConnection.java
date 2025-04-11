package db.jdbc.embedded;

import db.jdbc.ConnectionAdapter;
import db.planner.Planner;
import db.server.HAPdb;
import db.transaction.Transaction;

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
    public Statement createStatement() {
        return new EmbeddedStatement(this, planner);
    }

    @Override
    public void close() {
        commit();
    }

    @Override
    public void commit() {
        transaction.commit();
        transaction = db.getNewTransaction();

        System.out.println(db.getFileManager().fileStatistics().toString());
    }

    @Override
    public void rollback() {
        transaction.rollback();
        transaction = db.getNewTransaction();

        System.out.println(db.getFileManager().fileStatistics().toString());
    }

    Transaction getTransaction() {
        return transaction;
    }
}
