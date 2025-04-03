package db.transaction.recovery;

import db.file.Block;
import db.file.Page;
import db.log.LogManager;
import db.transaction.Transaction;

public class SetIntRecord implements LogRecord {
    private final int txNum;
    private final int offset;
    private final int value;
    private final Block block;

    public SetIntRecord(Page page) {
        int transactionPosition = Integer.BYTES;
        this.txNum = page.getInt(transactionPosition);

        int filePosition = transactionPosition + Integer.BYTES;
        String filename = page.getString(filePosition);

        int blockPosition = filePosition + Page.maxLength(filename.length());
        int blockNumber = page.getInt(blockPosition);
        this.block = new Block(filename, blockNumber);

        int offsetPosition = blockPosition + Integer.BYTES;
        offset = page.getInt(offsetPosition);

        int valuePosition = offsetPosition + Integer.BYTES;
        value = page.getInt(valuePosition);
    }

    /**
     * Logging only old value due to undo only recovery
     *
     * @return it return LSN
     */
    public static int writeToLog(LogManager logManager, int txNum, Block block, int offset, int value) {
        int transactionPosition = Integer.BYTES;
        int filePosition = transactionPosition + Integer.BYTES;
        int blockPosition = filePosition + Page.maxLength(block.fileName().length());
        int offsetPosition = blockPosition + Integer.BYTES;
        int valuePosition = offsetPosition + Integer.BYTES;
        int recordLength = valuePosition + Integer.BYTES;

        byte[] record = new byte[recordLength];
        Page page = new Page(record);
        page.setInt(0, RecordType.SETINT.ordinal());
        page.setInt(transactionPosition, txNum);
        page.setString(filePosition, block.fileName());
        page.setInt(blockPosition, block.blockNumber());
        page.setInt(offsetPosition, offset);
        page.setInt(valuePosition, value);

        return logManager.append(record);
    }

    @Override
    public RecordType getRecordType() {
        return RecordType.SETINT;
    }

    @Override
    public int getTxNumber() {
        return this.txNum;
    }

    @Override
    public void undo(Transaction transaction) {
        transaction.pin(block);
        transaction.setInt(block, offset, value, false);
        transaction.unpin(block);
    }
}
