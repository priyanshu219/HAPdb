package db.planner;

import db.parser.*;
import db.transaction.Transaction;

public interface UpdatePlanner {
    int executeInsert(InsertData data, Transaction transaction);

    int executeDelete(DeleteData data, Transaction transaction);

    int executeModify(ModifyData data, Transaction transaction);

    int executeCreateTable(CreateTableData data, Transaction transaction);

    int executeCreateView(CreateViewData data, Transaction transaction);

    int executeCreateIndex(CreateIndexData data, Transaction transaction);
}
