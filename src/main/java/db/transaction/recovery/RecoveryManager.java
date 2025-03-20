package db.transaction.recovery;

import db.buffer.Buffer;
import db.buffer.BufferManager;
import db.file.Block;
import db.log.LogManager;
import db.transaction.Transaction;
import db.transaction.recovery.LogRecord.RecordType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

public class RecoveryManager {
    private final LogManager logManager;
    private final BufferManager bufferManager;
    private final Transaction transaction;
    private final int txNum;

    public RecoveryManager(LogManager logManager, BufferManager bufferManager, Transaction transaction, int txNum) {
        this.transaction = transaction;
        this.txNum = txNum;
        this.logManager = logManager;
        this.bufferManager = bufferManager;
        StartRecord.writeToLog(logManager, txNum);
    }

    public void commit() {
        bufferManager.flushAll(txNum);
        int LSN = CommitRecord.writeToLog(logManager, txNum);
        logManager.flush(LSN);
    }

    public void rollback() throws IOException {
        doRollback();
        bufferManager.flushAll(txNum);
        int LSN = RollbackRecord.writeToLog(logManager, txNum);
        logManager.flush(LSN);
    }

    public void recover() throws IOException {
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

    private void doRollback() throws IOException {
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

    private void doRecover() throws IOException {
        Collection<Integer> finishedTxs = new ArrayList<>();
        Iterator<byte[]> iterator = logManager.iterator();

        while (iterator.hasNext()) {
            byte[] bytes = iterator.next();
            LogRecord record = LogRecord.createLogRecord(bytes);
            if (record.getRecordType() == RecordType.CHECKPOINT) {
                return;
            }
            if (record.getRecordType() == RecordType.COMMIT || record.getRecordType() == RecordType.ROLLBACK) {
                finishedTxs.add(record.getTxNumber());
            } else if (!finishedTxs.contains(record.getTxNumber())) {
                record.undo(transaction);
            }
        }
    }
}
