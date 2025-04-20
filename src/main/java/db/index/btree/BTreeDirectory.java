package db.index.btree;

import db.file.Block;
import db.query.Constant;
import db.record.Layout;
import db.transaction.Transaction;

public class BTreeDirectory {
    private final Transaction tx;
    private final Layout layout;
    private final String fileName;
    private BTPage page;

    public BTreeDirectory(Transaction tx, Block block, Layout layout) {
        this.tx = tx;
        this.layout = layout;
        page = new BTPage(tx, block, layout);
        fileName = block.fileName();
    }

    public void close() {
        page.close();
    }

    public int search(Constant searchKey) {
        Block childBlock = findChildBlock(searchKey);
        while (page.getFlag() > 0) {
            page.close();
            page = new BTPage(tx, childBlock, layout);
            childBlock = findChildBlock(searchKey);
        }

        return childBlock.blockNumber();
    }

    public void makeNewRoot(DirectorEntry entry) {
        Constant firstValue = page.getDataVal(0);
        int level = page.getFlag();
        Block newBlock = page.split(0, level);
        DirectorEntry oldRoot = new DirectorEntry(firstValue, newBlock.blockNumber());
        insertEntry(oldRoot);
        insertEntry(entry);
        page.setFlag(level + 1);
    }

    public DirectorEntry insert(DirectorEntry entry) {
        if (page.getFlag() == 0) {
            return insertEntry(entry);
        }
        Block childBlock = findChildBlock(entry.dataVal());
        BTreeDirectory child = new BTreeDirectory(tx, childBlock, layout);
        DirectorEntry newEntry = child.insert(entry);
        child.close();
        return (null != newEntry) ? insertEntry(newEntry) : null;
    }

    private Block findChildBlock(Constant searchKey) {
        int slot = page.findSlotBefore(searchKey);
        if (page.getDataVal(slot + 1).equals(searchKey)) {
            slot++;
        }
        int blockNumber = page.getChildNumber(slot);
        return new Block(fileName, blockNumber);
    }

    private DirectorEntry insertEntry(DirectorEntry entry) {
        int newSlot = page.findSlotBefore(entry.dataVal());
        page.insetDirectory(newSlot, entry.dataVal(), entry.blockNumber());
        if (!page.isFull()) {
            return null;
        }
        int level = page.getFlag();
        int splitPosition = page.getTotalRecords() / 2;
        Constant splitValue = page.getDataVal(splitPosition);
        Block newBlock = page.split(splitPosition, level);
        return new DirectorEntry(splitValue, newBlock.blockNumber());
    }
}
