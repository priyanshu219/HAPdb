package db.transaction.recovery;

import db.buffer.Buffer;
import db.buffer.BufferManager;
import db.file.Block;
import db.file.Page;
import db.log.LogManager;
import db.transaction.Transaction;
import db.transaction.recovery.LogRecord.RecordType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class RecoveryManager {
    private final LogManager logManager;
    private final BufferManager bufferManager;
    private final Transaction transaction;
    private final Set<Block> modifiedBlocks;
    private final int txNum;

    public RecoveryManager(LogManager logManager, BufferManager bufferManager, Transaction transaction, int txNum) {
        this.transaction = transaction;
        this.txNum = txNum;
        this.logManager = logManager;
        this.bufferManager = bufferManager;
        this.modifiedBlocks = new HashSet<>();
        StartRecord.writeToLog(logManager, txNum);
    }

    public void commit() {
        bufferManager.flushAll(txNum);
        int LSN = CommitRecord.writeToLog(logManager, txNum);
        logManager.flush(LSN);
    }

    public void rollback() {
        doRollback();
        bufferManager.flushAll(txNum);
        int LSN = RollbackRecord.writeToLog(logManager, txNum);
        logManager.flush(LSN);
    }

    public void recover() {
        doRecover();
        bufferManager.flushAll(txNum);
        int LSN = CheckpointRecord.writeToLog(logManager);
        logManager.flush(LSN);
    }

    public int setInt(Buffer buffer, int offset, int newValue) {
        int oldValue = buffer.getContents().getInt(offset);
        Block block = buffer.getBlock();
        return SetIntRecord.writeToLog(logManager, txNum, block, offset, oldValue);
    }

    public int setString(Buffer buffer, int offset, String newValue) {
        String oldValue = buffer.getContents().getString(offset);
        Block block = buffer.getBlock();
        return SetStringRecord.writeToLog(logManager, txNum, block, offset, oldValue);
    }

    public void backupBlock(Buffer buffer) {
        if (checkExistingModifiedBlocks(buffer.getBlock())) {
            return;
        }

        String fileName = takeBackup(buffer.getContents(), buffer.getBlock());
        BlockUpdateRecord.writeToLog(logManager, txNum, buffer.getBlock(), fileName);
    }

    private String takeBackup(Page page, Block block) {
        byte[] backUpBytes = page.getCopy();
        String backupFileName = "block_backups/tx" + txNum + "_" + block.fileName() + "_" + block.blockNumber() + ".bak";

        //TODO: Use filemanager
        try {
            Path path = Paths.get(backupFileName);
            Files.createDirectories(path.getParent());
            Files.write(path, backUpBytes);

            return backupFileName;
        } catch (IOException ex) {
            throw new RuntimeException("Failed to write block backup", ex);
        }
    }

    private boolean checkExistingModifiedBlocks(Block block) {
        return modifiedBlocks.contains(block);
    }

    private void doRollback() {
        Iterator<byte[]> iterator = logManager.iterator();
        while (iterator.hasNext()) {
            byte[] bytes = iterator.next();
            LogRecord record = LogRecord.createLogRecord(bytes);
            if (record.getTxNumber() == txNum) {
                if (record.getRecordType() == RecordType.START) {
                    return;
                }
                record.undo(transaction);
            }
        }
    }

    private void doRecover() {
        Set<Integer> finishedTxs = new HashSet<>();
        Iterator<byte[]> iterator = logManager.iterator();

        while (iterator.hasNext()) {
            byte[] bytes = iterator.next();
            LogRecord record = LogRecord.createLogRecord(bytes);
            if (record.getRecordType() == RecordType.CHECKPOINT) {
                return;
            }
            if (record.getRecordType() == RecordType.COMMIT || record.getRecordType() == RecordType.ROLLBACK) {
                finishedTxs.add(record.getTxNumber());
            } else if (record.getRecordType() == RecordType.BLOCK_UPDATE && !finishedTxs.contains(record.getTxNumber())) {
                record.undo(transaction);
            }
        }
    }
}
