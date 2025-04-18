package db.transaction;

import db.buffer.Buffer;
import db.buffer.BufferManager;
import db.file.Block;
import db.file.FileManager;
import db.file.Page;
import db.log.LogManager;
import db.transaction.concurrency.ConcurrencyManager;
import db.transaction.recovery.RecoveryManager;

import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;

public class Transaction {
    private static final int END_OF_FILE = -1;

    private static final Set<Transaction> activeTxns = ConcurrentHashMap.newKeySet();
    private static final Object CHECKPOINT_LOCK = new Object();
    private static final int CHECKPOINT_FREQUENCY = 10;
    private static final AtomicInteger txnCount = new AtomicInteger(0);

    private static int nextTxNum = 0;
    private static boolean checkpointInProgress = false;
    private final FileManager fileManager;
    private final RecoveryManager recoveryManager;
    private final BufferManager bufferManager;
    private final int txNum;
    private final ConcurrencyManager concurrencyManager;
    private final BufferList myBuffers;

    private final List<Block> appendedBlocks;

    public List<Block> getAppendedBlocks() {
        return appendedBlocks;
    }

    public Transaction(FileManager fileManager, LogManager logManager, BufferManager bufferManager) {
        synchronized (CHECKPOINT_LOCK) {
            while (checkpointInProgress) {
                try {
                    CHECKPOINT_LOCK.wait();
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }
            activeTxns.add(this);
        }

        this.fileManager = fileManager;
        this.bufferManager = bufferManager;
        this.txNum = nextTxNumber();
        this.recoveryManager = new RecoveryManager(logManager, bufferManager, this, txNum);
        this.concurrencyManager = new ConcurrencyManager(txNum);
        this.myBuffers = new BufferList(bufferManager);
        this.appendedBlocks = new ArrayList<>();

        int cnt = txnCount.incrementAndGet();
        if (cnt % CHECKPOINT_FREQUENCY == 0) {
            runQuiescentCheckpoint();
        }
    }

    private static synchronized int nextTxNumber() {
        nextTxNum++;
        System.out.println("new transaction: " + nextTxNum);
        return nextTxNum;
    }

    public static void addQuiescentCheckpoint(RecoveryManager recoveryManager) {
        synchronized (CHECKPOINT_LOCK) {
            checkpointInProgress = true;
            while (!activeTxns.isEmpty()) {
                try {
                    CHECKPOINT_LOCK.wait();
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }


            recoveryManager.setCheckpoint();
            checkpointInProgress = false;
            CHECKPOINT_LOCK.notifyAll();
        }
    }

    public void commit() {
        recoveryManager.commit();
        concurrencyManager.release();
        myBuffers.unpinAll();
        System.out.println("transaction " + txNum + " committed");

        synchronized (CHECKPOINT_LOCK) {
            activeTxns.remove(this);
            if (activeTxns.isEmpty()) {
                CHECKPOINT_LOCK.notifyAll();
            }
        }
    }

    public void rollback() {
        recoveryManager.rollback();
        concurrencyManager.release();
        myBuffers.unpinAll();
        System.out.println("transaction " + txNum + " rolled back");

        synchronized (CHECKPOINT_LOCK) {
            activeTxns.remove(this);
            if (activeTxns.isEmpty()) {
                CHECKPOINT_LOCK.notifyAll();
            }
        }
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
            recoveryManager.backupBlock(buffer);
            LSN = recoveryManager.setString(buffer, offset);
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
            LSN = recoveryManager.setInt(buffer, offset);
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

        Block appendedBlock = fileManager.append(fileName);
        appendedBlocks.add(appendedBlock);
        recoveryManager.appendBlock(appendedBlock);

        return appendedBlock;
    }

    public void truncate(Block block) {
        fileManager.truncate(block);
    }

    public int getBlockSize() {
        return FileManager.getBlockSize();
    }

    public int availableBuffers() {
        return bufferManager.getTotalAvailable();
    }

    public void setBytes(Block block, byte[] bytes) {
        Page page = new Page(bytes);
        concurrencyManager.xLock(block);
        fileManager.write(block, page);
    }

    public void runQuiescentCheckpoint() {
        new Thread(() -> addQuiescentCheckpoint(recoveryManager)).start();
    }
}
