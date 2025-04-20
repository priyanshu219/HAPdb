package db.index.btree;

import db.file.Block;
import db.query.Constant;
import db.record.Layout;
import db.record.RID;
import db.record.Schema;
import db.transaction.Transaction;

import static java.sql.Types.INTEGER;

public class BTPage {
    private final Transaction tx;
    private final Layout layout;
    private Block currentBlock;

    public BTPage(Transaction tx, Block currentBlock, Layout layout) {
        this.tx = tx;
        this.currentBlock = currentBlock;
        this.layout = layout;
        tx.pin(currentBlock);
    }

    public int findSlotBefore(Constant searchKey) {
        int slot = 0;
        while (slot < getTotalRecords() && getDataVal(slot).compareTo(searchKey) < 0) {
            slot++;
        }
        return (slot - 1);
    }

    public Constant getDataVal(int slot) {
        return getValue(slot, "dataVal");
    }

    public int getTotalRecords() {
        return tx.getInt(currentBlock, Integer.BYTES);
    }

    private void setTotalRecords(int count) {
        tx.setInt(currentBlock, Integer.BYTES, count, true);
    }

    public void close() {
        if (null != currentBlock) {
            tx.unpin(currentBlock);
        }
        currentBlock = null;
    }

    public boolean isFull() {
        return slotPosition(getTotalRecords() + 1) >= tx.getBlockSize();
    }

    public Block split(int splitPosition, int flag) {
        Block newBlock = appendNew(flag);
        BTPage newPage = new BTPage(tx, newBlock, layout);
        transferRecords(splitPosition, newPage);
        newPage.setFlag(flag);
        newPage.close();
        return newBlock;
    }

    public Block appendNew(int flag) {
        Block block = tx.append(currentBlock.fileName());
        tx.pin(block);
        format(block, flag);
        return block;
    }

    public void format(Block block, int flag) {
        tx.setInt(block, 0, flag, false);
        tx.setInt(block, Integer.BYTES, 0, false);

        int recordSize = layout.getSlotSize();
        for (int position = 2 * Integer.BYTES; position + recordSize <= tx.getBlockSize(); position += recordSize) {
            makeDefaultRecord(block, position);
        }
    }

    public int getFlag() {
        return tx.getInt(currentBlock, 0);
    }

    public void setFlag(int value) {
        tx.setInt(currentBlock, 0, value, true);
    }

    public RID getDataRid(int slot) {
        return new RID(getInt(slot, "block"), getInt(slot, "id"));
    }

    public void delete(int slot) {
        for (int i = slot + 1; i < getTotalRecords(); i++) {
            copyRecord(i, i - 1);
        }
        setTotalRecords(getTotalRecords() - 1);
    }

    public void insertLeaf(int slot, Constant value, RID rid) {
        insert(slot);
        setValue(slot, "dataVal", value);
        setInt(slot, "block", rid.blockNum());
        setInt(slot, "id", rid.slot());
    }

    public int getChildNumber(int slot) {
        return getInt(slot, "block");
    }

    public void insetDirectory(int slot, Constant value, int blockNumber) {
        insert(slot);
        setValue(slot, "dataVal", value);
        setInt(slot, "block", blockNumber);
    }

    private Constant getValue(int slot, String fieldName) {
        int type = layout.getSchema().getFieldType(fieldName);
        if (type == INTEGER) {
            return new Constant(getInt(slot, fieldName));
        } else {
            return new Constant(getString(slot, fieldName));
        }
    }

    private int getInt(int slot, String fieldName) {
        int position = fieldPosition(slot, fieldName);
        return tx.getInt(currentBlock, position);
    }

    private String getString(int slot, String fieldName) {
        int position = fieldPosition(slot, fieldName);
        return tx.getString(currentBlock, position);
    }

    private void setValue(int slot, String fieldName, Constant value) {
        int type = layout.getSchema().getFieldType(fieldName);
        if (type == INTEGER) {
            setInt(slot, fieldName, value.asInt());
        } else {
            setString(slot, fieldName, value.asString());
        }
    }

    private void setInt(int slot, String fieldName, int value) {
        int position = fieldPosition(slot, fieldName);
        tx.setInt(currentBlock, position, value, true);
    }

    private void setString(int slot, String fieldName, String value) {
        int position = fieldPosition(slot, fieldName);
        tx.setString(currentBlock, position, value, true);
    }

    private int fieldPosition(int slot, String fieldName) {
        int offset = layout.getFieldOffset(fieldName);
        return slotPosition(slot) + offset;
    }

    private int slotPosition(int slot) {
        int slotSize = layout.getSlotSize();
        return (Integer.BYTES + Integer.BYTES + (slot * slotSize));
    }

    private void makeDefaultRecord(Block block, int position) {
        for (String fieldName : layout.getSchema().getFields()) {
            int offset = layout.getFieldOffset(fieldName);
            if (layout.getSchema().getFieldType(fieldName) == INTEGER) {
                tx.setInt(block, position + offset, 0, false);
            } else {
                tx.setString(block, position + offset, "", false);
            }
        }
    }

    private void transferRecords(int slot, BTPage destinationPage) {
        int destinationSlot = 0;
        while (slot < getTotalRecords()) {
            destinationPage.insert(destinationSlot);
            Schema schema = layout.getSchema();
            for (String fieldName : schema.getFields()) {
                destinationPage.setValue(destinationSlot, fieldName, getValue(slot, fieldName));
            }
            delete(slot);
            destinationSlot++;
        }
    }

    private void insert(int slot) {
        for (int i = getTotalRecords(); i > slot; i--) {
            copyRecord(i - 1, i);
        }
        setTotalRecords(getTotalRecords() + 1);
    }

    private void copyRecord(int from, int to) {
        Schema schema = layout.getSchema();
        for (String fieldName : schema.getFields()) {
            setValue(to, fieldName, getValue(from, fieldName));
        }
    }
}
