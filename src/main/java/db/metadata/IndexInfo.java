package db.metadata;

import db.index.HashIndex;
import db.index.Index;
import db.record.Layout;
import db.record.Schema;
import db.transaction.Transaction;

import static java.sql.Types.INTEGER;

public class IndexInfo {
    private final String indexName;
    private final String fieldName;
    private final Layout tableLayout;
    private final Layout indexLayout;
    private final StatInfo statInfo;
    private final Transaction transaction;

    public IndexInfo(String indexName, String fieldName, Layout tableLayout, Transaction transaction, StatInfo statInfo) {
        this.indexName = indexName;
        this.fieldName = fieldName;
        this.transaction = transaction;
        this.statInfo = statInfo;
        this.tableLayout = tableLayout;
        this.indexLayout = createIndexLayout();
    }

    public Index open() {
        Schema schema = new Schema();
        return new HashIndex(transaction, indexName, indexLayout);
    }

    public int blockAccessed() {
        int recordsPerBlock = transaction.getBlockSize() / indexLayout.getSlotSize();
        int numBlocks = statInfo.recordsOutput() / recordsPerBlock;
        return HashIndex.searchCost(numBlocks, recordsPerBlock);
    }

    public int recordsOutput() {
        return statInfo.recordsOutput() / statInfo.getDistinctValues(fieldName);
    }

    public int distinctValues(String fieldName) {
        return this.fieldName.equals(fieldName) ? 1 : statInfo.getDistinctValues(this.fieldName);
    }

    private Layout createIndexLayout() {
        Schema schema = new Schema();
        schema.addIntField("block");
        schema.addIntField("id");

        if (tableLayout.getSchema().getFieldType(fieldName) == INTEGER) {
            schema.addIntField("dataVal");
        } else {
            int fieldLength = tableLayout.getSchema().getFieldLength(fieldName);
            schema.addStringField("dataVal", fieldLength);
        }

        return new Layout(schema);
    }
}
