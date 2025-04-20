package db.index.btree;

import db.file.Block;
import db.query.Constant;
import db.record.Layout;
import db.record.RID;
import db.transaction.Transaction;

public class BTreeLeaf {
    private final Transaction tx;
    private final Layout layout;
    private final Constant searchKey;
    private final String fileName;
    private BTPage page;
    private int currentSlot;

    public BTreeLeaf(Transaction tx, Block block, Layout layout, Constant searchKey) {
        this.tx = tx;
        this.layout = layout;
        this.searchKey = searchKey;
        page = new BTPage(tx, block, layout);
        currentSlot = page.findSlotBefore(searchKey);
        this.fileName = block.fileName();
    }

    public void close() {
        page.close();
    }

    public boolean next() {
        currentSlot++;
        if (currentSlot >= page.getTotalRecords()) {
            return tryOverflow();
        } else if (page.getDataVal(currentSlot).equals(searchKey)) {
            return true;
        } else {
            return tryOverflow();
        }
    }

    public RID getDataRid() {
        return page.getDataRid(currentSlot);
    }

    public void delete(RID dataRID) {
        while (next()) {
            if (getDataRid().equals(dataRID)) {
                page.delete(currentSlot);
                return;
            }
        }
    }

    public DirectorEntry insert(RID dataRid) {
        if (page.getFlag() >= 0 && page.getDataVal(0).compareTo(searchKey) > 0) {
            Constant firstValue = page.getDataVal(0);
            Block newBlock = page.split(0, page.getFlag());
            currentSlot = 0;
            page.setFlag(-1);
            page.insertLeaf(currentSlot, searchKey, dataRid);
            return new DirectorEntry(firstValue, newBlock.blockNumber());
        }

        currentSlot++;
        page.insertLeaf(currentSlot, searchKey, dataRid);

        if (!page.isFull()) {
            return null;
        }

        Constant firstKey = page.getDataVal(0);
        Constant lastKey = page.getDataVal(page.getTotalRecords() - 1);

        if (lastKey.equals(firstKey)) {
            Block newBlock = page.split(1, page.getFlag());
            page.setFlag(newBlock.blockNumber());
            return null;
        } else {
            int splitPosition = page.getTotalRecords() / 2;
            Constant splitKey = page.getDataVal(splitPosition);
            if (splitKey.equals(firstKey)) {
                while (page.getDataVal(splitPosition).equals(splitKey)) {
                    splitPosition++;
                }
                splitKey = page.getDataVal(splitPosition);
            } else {
                while (page.getDataVal(splitPosition - 1).equals(splitKey)) {
                    splitPosition--;
                }
            }
            Block newBlock = page.split(splitPosition, -1);
            return new DirectorEntry(splitKey, newBlock.blockNumber());
        }
    }

    private boolean tryOverflow() {
        Constant firstKey = page.getDataVal(0);
        int flag = page.getFlag();
        if (!searchKey.equals(firstKey) || flag < 0) {
            return false;
        }
        page.close();
        Block nextBlock = new Block(fileName, flag);
        page = new BTPage(tx, nextBlock, layout);
        currentSlot = 0;

        return true;
    }
}
