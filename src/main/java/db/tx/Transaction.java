package db.tx;

import db.buffer.Buffer;
import db.buffer.BufferManager;
import db.file.Block;
import db.file.FileManager;
import db.file.Page;
import db.log.LogManager;
import db.tx.concurrency.ConcurrencyManager;
import db.tx.recovery.RecoveryManager;

import java.io.IOException;

public class Transaction {
    private static int nextTxNum = 0;
    private static final int END_OF_FILE = -1;
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

    public void commit() {
        recoveryManager.commit();
        concurrencyManager.release();
        myBuffers.unpinAll();
        System.out.println("transaction " + txNum + " committed");
    }

    public void rollback() throws IOException {
        recoveryManager.rollback();
        concurrencyManager.release();
        myBuffers.unpinAll();
        System.out.println("transaction " + txNum + " rolled back");
    }

    public void recover() throws IOException {
        bufferManager.flushAll(txNum);
        recoveryManager.recover();
    }

    public void pin(Block block) throws IOException {
        myBuffers.pin(block);
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
            LSN = recoveryManager.setInt(buffer, offset, value);
        }
        Page page = buffer.getContents();
        page.setInt(offset, value);
        buffer.setModified(txNum, LSN);
    }

    public int getInt(Block block, int offset) {
        concurrencyManager.sLock(block);
        Buffer buffer = myBuffers.getBuffer(block);
        return buffer.getContents().getInt(offset);
    }

    public String getString(Block block, int offset) {
        concurrencyManager.sLock(block);
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

    public int blockSize() {
        return fileManager.getBlocksize();
    }

    public int availableBuffers() {
        return bufferManager.getTotalAvailable();
    }

    private static synchronized int nextTxNumber() {
        nextTxNum++;
        System.out.println("new transaction: " + nextTxNum);
        return nextTxNum;
    }
}
