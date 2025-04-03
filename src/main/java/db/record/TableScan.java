package db.record;

import db.file.Block;
import db.query.Constant;
import db.query.UpdateScan;
import db.transaction.Transaction;

import static java.sql.Types.INTEGER;

public class TableScan implements UpdateScan {
    private final Transaction transaction;
    private final Layout layout;
    private final String fileName;
    private RecordPage page;
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

    @Override
    public void close() {
        if (null != page) {
            transaction.unpin(page.getBlock());
        }
    }


    /**
     * methods that establish the current record
     */
    @Override
    public void beforeFirst() {
        moveToBlock(0);
    }

    @Override
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

    @Override
    public void moveToRID(RID rid) {
        close();
        Block block = new Block(fileName, rid.blockNum());
        page = new RecordPage(transaction, block, layout);
        currentSlot = rid.slot();
    }

    @Override
    public RID getRID() {
        return new RID(page.getBlock().blockNumber(), currentSlot);
    }

    @Override
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
    @Override
    public int getInt(String fieldName) {
        return page.getInt(currentSlot, fieldName);
    }

    @Override
    public String getString(String fieldName) {
        return page.getString(currentSlot, fieldName);
    }

    @Override
    public Constant getValue(String fieldName) {
        if (layout.getSchema().getFieldType(fieldName) == INTEGER) {
            return new Constant(getInt(fieldName));
        } else {
            return new Constant(getString(fieldName));
        }
    }

    @Override
    public boolean hasField(String fieldName) {
        return layout.getSchema().hasField(fieldName);
    }

    @Override
    public void setInt(String fieldName, int value) {
        page.setInt(currentSlot, fieldName, value);
    }

    @Override
    public void setString(String fieldName, String value) {
        page.setString(currentSlot, fieldName, value);
    }

    @Override
    public void setValue(String fieldName, Constant value) {
        if (layout.getSchema().getFieldType(fieldName) == INTEGER) {
            page.setInt(currentSlot, fieldName, value.asInt());
        } else {
            page.setString(currentSlot, fieldName, value.asString());
        }
    }

    @Override
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
