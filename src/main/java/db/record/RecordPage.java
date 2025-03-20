package db.record;

import db.file.Block;
import db.tx.Transaction;

import java.io.IOException;
import static java.sql.Types.INTEGER;


/**
 * Implementing fixed-size non-spanned homogeneous slotted page
 */
public class RecordPage {
    enum Flag {
        USED,
        EMPTY
    }

    private final Transaction transaction;
    private final Block block;
    private final Layout layout;

    public RecordPage(Transaction transaction, Block block, Layout layout) throws IOException {
        this.transaction = transaction;
        this.block = block;
        this.layout = layout;
        transaction.pin(block);
    }

    public int getInt(int slot, String fieldName) {
        int fieldPosition = offset(slot) + layout.getFieldOffset(fieldName);
        return transaction.getInt(block, fieldPosition);
    }

    public String getString(int slot, String fieldName) {
        int fieldPosition = offset(slot) + layout.getFieldOffset(fieldName);
        return transaction.getString(block, fieldPosition);
    }

    public void setInt(int slot, String fieldName, int value) {
        int fieldPosition = offset(slot) + layout.getFieldOffset(fieldName);
        transaction.setInt(block, fieldPosition, value, true);
    }

    public void setString(int slot, String fieldName, String value) {
        int fieldPosition = offset(slot) + layout.getFieldOffset(fieldName);
        transaction.setString(block, fieldPosition, value, true);
    }

    public void delete(int slot) {
        setFlag(slot, Flag.EMPTY);
    }

    public void format() {
        int slot = 0;
        while (isValidSlot(slot)) {
            transaction.setInt(block, offset(slot), Flag.EMPTY.ordinal(), false);
            Schema schema = layout.getSchema();
            for (String fieldName : schema.getFields()) {
                int fieldPosition = offset(slot) + layout.getFieldOffset(fieldName);
                if (schema.getFieldType(fieldName) == INTEGER) {
                    transaction.setInt(block, fieldPosition, 0, false);
                } else {
                    transaction.setString(block, fieldPosition, "", false);
                }
            }
            slot++;
        }
    }

    public int nextAfter(int slot) {
        return searchAfter(slot, Flag.USED);
    }

    public int insertAfter(int slot) {
        int newSlot = searchAfter(slot, Flag.EMPTY);
        if (newSlot >= 0) {
            setFlag(newSlot, Flag.USED);
        }
        return newSlot;
    }

    public Block getBlock() {
        return block;
    }

    private int searchAfter(int slot, Flag flag) {
        slot++;
        while (isValidSlot(slot)) {
            if (transaction.getInt(block, offset(slot)) == flag.ordinal()) {
                return slot;
            }
            slot++;
        }
        return -1;
    }

    private boolean isValidSlot(int slot) {
        return (offset(slot + 1) <= transaction.getBlockSize());
    }

    private void setFlag(int slot, Flag flag) {
        transaction.setInt(block, offset(slot), flag.ordinal(), true);
    }

    private int offset(int slot) {
        return slot * layout.getSlotSize();
    }
}
