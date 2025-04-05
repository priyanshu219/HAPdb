package db.planner;

import db.metadata.MetadataManager;
import db.parser.*;
import db.query.Constant;
import db.query.UpdateScan;
import db.transaction.Transaction;

import java.util.Iterator;

public class BasicUpdatePlanner implements UpdatePlanner {
    private final MetadataManager metadataManager;

    public BasicUpdatePlanner(MetadataManager metadataManager) {
        this.metadataManager = metadataManager;
    }

    @Override
    public int executeInsert(InsertData data, Transaction transaction) {
        Plan plan = new TablePlan(transaction, data.tableName(), metadataManager);
        UpdateScan updateScan = (UpdateScan) plan.open();
        updateScan.insert();
        Iterator<Constant> valIter = data.values().iterator();
        for (String fieldName : data.fields()) {
            Constant value = valIter.next();
            updateScan.setValue(fieldName, value);
        }
        updateScan.close();

        return 1;
    }

    @Override
    public int executeDelete(DeleteData data, Transaction transaction) {
        Plan plan = new TablePlan(transaction, data.tableName(), metadataManager);
        plan = new SelectPlan(plan, data.predicate());
        UpdateScan updateScan = (UpdateScan) plan.open();

        int cnt = 0;
        while (updateScan.next()) {
            updateScan.delete();
            cnt++;
        }
        updateScan.close();
        return cnt;
    }

    @Override
    public int executeModify(ModifyData data, Transaction transaction) {
        Plan plan = new TablePlan(transaction, data.tableName(), metadataManager);
        plan = new SelectPlan(plan, data.predicate());
        UpdateScan updateScan = (UpdateScan) plan.open();

        int cnt = 0;
        while (updateScan.next()) {
            Constant value = data.newValue().evaluate(updateScan);
            updateScan.setValue(data.targetField(), value);
            cnt++;
        }

        updateScan.close();
        return cnt;
    }

    @Override
    public int executeCreateTable(CreateTableData data, Transaction transaction) {
        metadataManager.createTable(data.tableName(), data.newSchema(), transaction);
        return 0;
    }

    @Override
    public int executeCreateView(CreateViewData data, Transaction transaction) {
        metadataManager.createView(data.viewName(), data.viewDef(), transaction);
        return 0;
    }

    @Override
    public int executeCreateIndex(CreateIndexData data, Transaction transaction) {
        return 0;
    }
}
