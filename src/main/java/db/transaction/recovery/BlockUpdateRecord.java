package db.transaction.recovery;

import db.file.Block;
import db.file.Page;
import db.log.LogManager;
import db.transaction.Transaction;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class BlockUpdateRecord implements LogRecord {
    private final int transactionNum;
    private final Block block;
    private final String value;

    public BlockUpdateRecord(Page page) {
        int txPosition = Integer.BYTES;
        transactionNum = page.getInt(txPosition);

        int filePosition = txPosition + Integer.BYTES;
        String fileName = page.getString(filePosition);

        int blockPosition = filePosition + Page.maxLength(fileName.length());
        int blockNumber = page.getInt(blockPosition);

        block = new Block(fileName, blockNumber);

        int valuePosition = blockPosition + Integer.BYTES;
        value = page.getString(valuePosition);
    }

    public static void writeToLog(LogManager logManager, int transactionNum, Block block, String value) {
        int txPosition = Integer.BYTES;
        int filePosition = txPosition + Integer.BYTES;
        int blockPosition = filePosition + Page.maxLength(block.fileName().length());
        int valuePosition = blockPosition + Integer.BYTES;
        int recordLen = valuePosition + Page.maxLength(value.length());

        byte[] record = new byte[recordLen];
        Page page = new Page(record);
        page.setInt(0, RecordType.BLOCK_UPDATE.ordinal());
        page.setInt(txPosition, transactionNum);
        page.setString(filePosition, block.fileName());
        page.setInt(blockPosition, block.blockNumber());
        page.setString(valuePosition, value);

        logManager.append(record);
    }

    @Override
    public RecordType getRecordType() {
        return RecordType.BLOCK_UPDATE;
    }

    @Override
    public int getTxNumber() {
        return transactionNum;
    }

    @Override
    public void undo(Transaction transaction) {
        try {
            transaction.pin(block);

            Path path = Paths.get(value);
            byte[] backupData = Files.readAllBytes(path);

            transaction.setBytes(block, backupData);
            transaction.unpin(block);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
