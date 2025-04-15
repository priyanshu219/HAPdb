package db.transaction.recovery;

import db.file.Block;
import db.file.Page;
import db.log.LogManager;
import db.transaction.Transaction;

public class AppendBlockRecord implements LogRecord{
    private final int txNum;
    private final Block block;

    public AppendBlockRecord(Page page) {
        int txNumPosition = Integer.BYTES;
        this.txNum = page.getInt(txNumPosition);

        int fileNamePosition = txNumPosition + Integer.BYTES;
        String fileName = page.getString(fileNamePosition);

        int blockPosition = fileNamePosition + Page.maxLength(fileName.length());
        int blockNum = page.getInt(blockPosition);
        block = new Block(fileName, blockNum);
    }
    @Override
    public RecordType getRecordType() {
        return RecordType.APPEND_BLOCK;
    }

    @Override
    public int getTxNumber() {
        return txNum;
    }

    public static int writeToLog(LogManager logManager, int txNum, Block block) {
        int txNumPosition = Integer.BYTES;
        int fileNamePosition = txNumPosition + Integer.BYTES;
        int blockPosition = fileNamePosition + Page.maxLength(block.fileName().length());
        int recordLength = blockPosition + Integer.BYTES;

        byte[] record = new byte[recordLength];
        Page page = new Page(record);

        page.setInt(0, RecordType.APPEND_BLOCK.ordinal());
        page.setInt(txNumPosition, txNum);
        page.setString(fileNamePosition, block.fileName());
        page.setInt(blockPosition, block.blockNumber());

        return logManager.append(record);
    }

    @Override
    public void undo(Transaction transaction) {
        transaction.truncate(block);
    }
}
