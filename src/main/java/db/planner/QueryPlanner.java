package db.planner;

import db.parser.QueryData;
import db.transaction.Transaction;

public interface QueryPlanner {
    Plan createPlan(QueryData data, Transaction transaction);
}
