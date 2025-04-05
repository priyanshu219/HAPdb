package db.planner;

import db.metadata.MetadataManager;
import db.parser.Parser;
import db.parser.QueryData;
import db.transaction.Transaction;

import java.util.ArrayList;
import java.util.List;

public class BasicQueryPlanner implements QueryPlanner {
    private final MetadataManager metadataManager;

    public BasicQueryPlanner(MetadataManager metadataManager) {
        this.metadataManager = metadataManager;
    }

    @Override
    public Plan createPlan(QueryData data, Transaction transaction) {
        List<Plan> plans = new ArrayList<>();
        for (String tableName : data.tables()) {
            String viewDef = metadataManager.getViewDef(tableName, transaction);
            if (null != viewDef) {
                Parser parser = new Parser(viewDef);
                QueryData viewData = parser.query();
                plans.add(createPlan(viewData, transaction));
            }
        }

        Plan plan = plans.removeFirst();
        for (Plan nextPlan : plans) {
            plan = new ProductPlan(plan, nextPlan);
        }

        plan = new SelectPlan(plan, data.predicate());

        return new ProjectPlan(plan, data.fields());
    }
}
