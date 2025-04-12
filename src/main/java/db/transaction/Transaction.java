package db.transaction;

import db.buffer.Buffer;
import db.buffer.BufferManager;
import db.file.Block;
import db.file.FileManager;
import db.file.Page;
import db.log.LogManager;
import db.transaction.concurrency.ConcurrencyManager;
import db.transaction.recovery.RecoveryManager;

public class Transaction {
    private static final int END_OF_FILE = -1;
    private static int nextTxNum = 0;
    private final FileManager fileManager;
    private final RecoveryManager recoveryManager;
    private final BufferManager bufferManager;
    private final int txNum;
    private final ConcurrencyManager concurrencyManager;
    private final BufferList myBuffers;

    public Transaction(FileManager fileManager, LogManager logManager, BufferManager bufferManager) {
        this.fileManager = fileManager;
        this.bufferManager = bufferManager;
        this.txNum = nextTxNumber();
        this.recoveryManager = new RecoveryManager(logManager, bufferManager, this, txNum);
        this.concurrencyManager = new ConcurrencyManager();
        myBuffers = new BufferList(bufferManager);
    }

    private static synchronized int nextTxNumber() {
        nextTxNum++;
        System.out.println("new transaction: " + nextTxNum);
        return nextTxNum;
    }

    public void commit() {
        recoveryManager.commit();
        concurrencyManager.release();
        myBuffers.unpinAll();
        System.out.println("transaction " + txNum + " committed");
    }

    public void rollback() {
        recoveryManager.rollback();
        concurrencyManager.release();
        myBuffers.unpinAll();
        System.out.println("transaction " + txNum + " rolled back");
    }

    public void recover() {
        bufferManager.flushAll(txNum);
        recoveryManager.recover();

        try {
            LogArchiver.archiveLog("hapdb.log", "log_archive");
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void pin(Block block) {
        myBuffers.pin(block);
        concurrencyManager.sLock(block);
    }

    public void setString(Block block, int offset, String value, boolean okToLog) {
        concurrencyManager.xLock(block);
        Buffer buffer = myBuffers.getBuffer(block);
        int LSN = -1;
        if (okToLog) {
            LSN = recoveryManager.setString(buffer, offset, value);
        }
        Page page = buffer.getContents();
        page.setString(offset, value);
        buffer.setModified(txNum, LSN);
    }

    public void unpin(Block block) {
        myBuffers.unpin(block);
    }

    public void setInt(Block block, int offset, int value, boolean okToLog) {
        concurrencyManager.xLock(block);
        Buffer buffer = myBuffers.getBuffer(block);
        int LSN = -1;
        if (okToLog) {
            recoveryManager.backupBlock(buffer);
            LSN = recoveryManager.setInt(buffer, offset, value);
        }
        Page page = buffer.getContents();
        page.setInt(offset, value);
        buffer.setModified(txNum, LSN);
    }

    public int getInt(Block block, int offset) {
//        concurrencyManager.sLock(block);
        Buffer buffer = myBuffers.getBuffer(block);
        return buffer.getContents().getInt(offset);
    }

    public String getString(Block block, int offset) {
//        concurrencyManager.sLock(block);
        Buffer buffer = myBuffers.getBuffer(block);
        return buffer.getContents().getString(offset);
    }

    public int size(String fileName) {
        Block dummyBlock = new Block(fileName, END_OF_FILE);
        concurrencyManager.sLock(dummyBlock);
        return fileManager.length(fileName);
    }

    public Block append(String fileName) {
        Block dummyBlock = new Block(fileName, END_OF_FILE);
        concurrencyManager.xLock(dummyBlock);
        return fileManager.append(fileName);
    }

    public int getBlockSize() {
        return FileManager.getBlockSize();
    }

    public int availableBuffers() {
        return bufferManager.getTotalAvailable();
    }

    public void setBytes(Block block, byte[] bytes) {
        Page page = new Page(bytes);
        fileManager.write(block, page);
    }
}
