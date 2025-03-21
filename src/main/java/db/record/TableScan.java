package db.record;

import db.file.Block;
import db.transaction.Transaction;

public class TableScan {
    private final Transaction transaction;
    private final Layout layout;
    private RecordPage page;
    private final String fileName;
    private int currentSlot;

    public TableScan(Transaction transaction, String tableName, Layout layout) {
        this.transaction = transaction;
        this.layout = layout;
        this.fileName = tableName + ".tbl";
        if (transaction.size(fileName) == 0) {
            moveToNewBlock();
        } else {
            moveToBlock(0);
        }
    }

    public void close() {
        if (null != page) {
            transaction.unpin(page.getBlock());
        }
    }



    /**
     * methods that establish the current record
     */
    public void beforeFirst() {
        moveToBlock(0);
    }

    public boolean next() {
        currentSlot = page.nextAfter(currentSlot);
        while (currentSlot < 0) {
            if (atLastBlock()) {
                return false;
            }
            moveToBlock(page.getBlock().blockNumber() + 1);
            currentSlot = page.nextAfter(currentSlot);
        }
        return true;
    }

    public void moveToRID(RID rid) {
        close();
        Block block = new Block(fileName, rid.blockNum());
        page = new RecordPage(transaction, block, layout);
        currentSlot = rid.slot();
    }

    public void insert() {
        currentSlot = page.insertAfter(currentSlot);
        while (currentSlot < 0) {
            if (atLastBlock()) {
                moveToNewBlock();
            } else {
                moveToBlock(page.getBlock().blockNumber() + 1);
            }
            currentSlot = page.insertAfter(currentSlot);
        }
    }

    /**
     * Methods that access the current record
     */
    public int getInt(String fieldName) {
        return page.getInt(currentSlot, fieldName);
    }

    public String getString(String fieldName) {
        return page.getString(currentSlot, fieldName);
    }

    public void setInt(String fieldName, int value) {
        page.setInt(currentSlot, fieldName, value);
    }

    public void setString(String fieldName, String value) {
        page.setString(currentSlot, fieldName, value);
    }

    public void delete() {
        page.delete(currentSlot);
    }

    private void moveToNewBlock() {
        close();
        Block block = transaction.append(fileName);
        page = new RecordPage(transaction, block, layout);
        page.format();
        currentSlot = -1;
    }

    private void moveToBlock(int blockNumber) {
        close();
        Block block = new Block(fileName, blockNumber);
        page = new RecordPage(transaction, block, layout);
        currentSlot = -1;
    }

    private boolean atLastBlock() {
        return (page.getBlock().blockNumber() == transaction.size(fileName) - 1);
    }
}
