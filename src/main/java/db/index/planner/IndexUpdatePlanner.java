package db.index.planner;

import db.index.Index;
import db.metadata.IndexInfo;
import db.metadata.MetadataManager;
import db.parser.*;
import db.planner.Plan;
import db.planner.SelectPlan;
import db.planner.TablePlan;
import db.planner.UpdatePlanner;
import db.query.Constant;
import db.query.UpdateScan;
import db.record.RID;
import db.transaction.Transaction;

import java.util.Iterator;
import java.util.Map;

public class IndexUpdatePlanner implements UpdatePlanner {
    private final MetadataManager metadataManager;

    public IndexUpdatePlanner(MetadataManager metadataManager) {
        this.metadataManager = metadataManager;
    }

    @Override
    public int executeInsert(InsertData data, Transaction transaction) {
        String tableName = data.tableName();
        Plan plan = new TablePlan(transaction, tableName, metadataManager);

        UpdateScan scan = (UpdateScan) plan.open();
        scan.insert();
        RID rid = scan.getRID();

        Map<String, IndexInfo> indexes = metadataManager.getIndexInfo(tableName, transaction);
        Iterator<Constant> valueIter = data.values().iterator();
        for (String fieldName : data.fields()) {
            Constant value = valueIter.next();
            scan.setValue(fieldName, value);

            IndexInfo indexInfo = indexes.get(fieldName);
            if (null != indexInfo) {
                Index index = indexInfo.open();
                index.insert(value, rid);
                index.close();
            }
        }

        scan.close();
        return 1;
    }

    @Override
    public int executeDelete(DeleteData data, Transaction transaction) {
        String tableName = data.tableName();
        Plan plan = new TablePlan(transaction, tableName, metadataManager);
        plan = new SelectPlan(plan, data.predicate());
        Map<String, IndexInfo> indexes = metadataManager.getIndexInfo(tableName, transaction);

        UpdateScan scan = (UpdateScan) plan.open();
        int count = 0;
        while (scan.next()) {
            RID rid = scan.getRID();
            for (String fieldName : indexes.keySet()) {
                Constant value = scan.getValue(fieldName);
                Index index = indexes.get(fieldName).open();
                index.delete(value, rid);
                index.close();
            }
            scan.delete();
            count++;
        }

        scan.close();
        return count;
    }

    @Override
    public int executeModify(ModifyData data, Transaction transaction) {
        String tableName = data.tableName();
        String fieldName = data.fieldName();
        Plan plan = new TablePlan(transaction, tableName, metadataManager);
        plan = new SelectPlan(plan, data.predicate());

        IndexInfo indexInfo = metadataManager.getIndexInfo(tableName, transaction).get(fieldName);
        Index index = (null == indexInfo) ? null : indexInfo.open();

        UpdateScan scan = (UpdateScan) plan.open();
        int count = 0;
        while (scan.next()) {
            Constant newValue = data.newValue().evaluate(scan);
            Constant oldValue = scan.getValue(fieldName);
            scan.setValue(fieldName, newValue);

            if (null != index) {
                RID rid = scan.getRID();
                index.delete(oldValue, rid);
                index.insert(newValue, rid);
            }
            count++;
        }

        if (null != index) {
            index.close();
        }
        scan.close();
        return count;
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
