package db.planner;

import db.parser.*;
import db.transaction.Transaction;

public class Planner {
    private final QueryPlanner queryPlanner;
    private final UpdatePlanner updatePlanner;

    public Planner(QueryPlanner queryPlanner, UpdatePlanner updatePlanner) {
        this.queryPlanner = queryPlanner;
        this.updatePlanner = updatePlanner;
    }

    public Plan createQuery(String command, Transaction transaction) {
        Parser parser = new Parser(command);
        QueryData queryData = parser.query();
        verifyQuery(queryData);

        return queryPlanner.createPlan(queryData, transaction);
    }

    public int executeUpdate(String command, Transaction transaction) {
        Parser parser = new Parser(command);
        Object object = parser.update();

        verifyUpdate(object);

        return switch (object) {
            case InsertData data -> updatePlanner.executeInsert(data, transaction);
            case DeleteData data -> updatePlanner.executeDelete(data, transaction);
            case ModifyData data -> updatePlanner.executeModify(data, transaction);
            case CreateTableData data -> updatePlanner.executeCreateTable(data, transaction);
            case CreateViewData data -> updatePlanner.executeCreateView(data, transaction);
            case CreateIndexData data -> updatePlanner.executeCreateIndex(data, transaction);
            default -> 0;
        };
    }

    private void verifyQuery(QueryData queryData) throws BadSyntaxException {
    }

    private void verifyUpdate(Object object) throws BadSyntaxException {
    }

}
