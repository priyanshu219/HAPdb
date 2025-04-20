package db.index.btree;

import db.file.Block;
import db.index.Index;
import db.query.Constant;
import db.record.Layout;
import db.record.RID;
import db.record.Schema;
import db.transaction.Transaction;

import static java.sql.Types.INTEGER;

public class BTreeIndex implements Index {
    private final Transaction tx;
    private final Layout directoryLayout;
    private final Layout leafLayout;
    private final String leafTable;
    private final Block rootBlock;
    private BTreeLeaf leaf;

    public BTreeIndex(Transaction tx, String indexName, Layout leafLayout) {
        this.tx = tx;
        leafTable = indexName + "leaf";
        this.leafLayout = leafLayout;
        if (tx.size(leafTable) == 0) {
            Block block = tx.append(leafTable);
            BTPage leafNode = new BTPage(tx, block, leafLayout);
            leafNode.format(block, -1);
        }

        Schema directorySchema = new Schema();
        directorySchema.add("block", leafLayout.getSchema());
        directorySchema.add("dataVal", leafLayout.getSchema());
        String directoryTable = indexName + "dir";

        directoryLayout = new Layout(directorySchema);
        rootBlock = new Block(directoryTable, 0);

        if (tx.size(directoryTable) == 0) {
            tx.append(directoryTable);
            BTPage node = new BTPage(tx, rootBlock, directoryLayout);
            node.format(rootBlock, 0);

            int fieldType = directorySchema.getFieldType("dataVal");
            Constant minimumVal = (fieldType == INTEGER) ? new Constant(Integer.MIN_VALUE) : new Constant("");
            node.insetDirectory(0, minimumVal, 0);
            node.close();
        }
    }

    public static int searchCost(int numBlocks, int rpd) {
        return 1 + (int) (Math.log(numBlocks) / Math.log(rpd));
    }

    @Override
    public void beforeFirst(Constant searchKey) {
        close();
        BTreeDirectory root = new BTreeDirectory(tx, rootBlock, directoryLayout);
        int blockNum = root.search(searchKey);
        root.close();
        Block leafBlock = new Block(leafTable, blockNum);
        leaf = new BTreeLeaf(tx, leafBlock, leafLayout, searchKey);
    }

    @Override
    public boolean next() {
        return leaf.next();
    }

    @Override
    public RID getDataRid() {
        return leaf.getDataRid();
    }

    @Override
    public void insert(Constant dataVal, RID dataRID) {
        beforeFirst(dataVal);
        DirectorEntry entry = leaf.insert(dataRID);
        leaf.close();

        if (null == entry) {
            return;
        }
        BTreeDirectory root = new BTreeDirectory(tx, rootBlock, directoryLayout);
        DirectorEntry entry2 = root.insert(entry);
        if (null != entry2) {
            root.makeNewRoot(entry2);
        }
        root.close();
    }

    @Override
    public void delete(Constant dataVal, RID dataRID) {
        beforeFirst(dataVal);
        leaf.delete(dataRID);
        leaf.close();
    }

    @Override
    public void close() {
        if (null != leaf) {
            leaf.close();
        }
    }
}
