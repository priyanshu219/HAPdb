package simpledb.buffer;

import simpledb.file.Block;
import simpledb.file.FileMgr;
import simpledb.log.LogMgr;

import java.io.IOException;

public class BufferMgr {
    private final Buffer[] bufferPool;
    private int totalAvailable;
    private static final long MAX_TIME = 10000;

    public BufferMgr(FileMgr fileMgr, LogMgr logMgr, int totalBuffers) {
        this.bufferPool = new Buffer[totalBuffers];
        this.totalAvailable = totalBuffers;
        for (int i = 0; i < totalBuffers; i++) {
            bufferPool[i] = new Buffer(fileMgr, logMgr);
        }
    }

    public synchronized int getTotalAvailable() {
        return totalAvailable;
    }

    public synchronized void flushAll(int txnNum) {
        for (Buffer buffer : bufferPool) {
            if (buffer.modifyingTx() == txnNum) {
                buffer.flush();
            }
        }
    }

    public synchronized void unpin(Buffer buffer) {
        buffer.unpin();
        if (!buffer.isPinned()) {
            totalAvailable++;
            notifyAll();
        }
    }

    public synchronized Buffer pin(Block block) throws IOException, BufferAbortException {
        try {
            long timestamp = System.currentTimeMillis();
            Buffer buffer = tryToPin(block);
            while (null == buffer && !waitingTooLong(timestamp)) {
                wait(MAX_TIME);
                buffer = tryToPin(block);
            }
            if (null == buffer) {
                throw new BufferAbortException();
            }
            return buffer;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean waitingTooLong(long startTime) {
        return (System.currentTimeMillis() - startTime) > MAX_TIME;
    }

    private Buffer tryToPin(Block block) throws IOException {
        Buffer buffer = findExistingBuffer(block);
        if (null == buffer) {
            buffer = chooseUpinnedBuffer();
            if (null == buffer) {
                return null;
            }
            buffer.assignToBlock(block);
        }
        if (!buffer.isPinned()) {
            totalAvailable--;
        }
        buffer.pin();
        return buffer;
    }

    private Buffer findExistingBuffer(Block block) {
        for (Buffer buffer : bufferPool) {
            Block newBlock = buffer.getBlock();
            if (null != newBlock && newBlock.equals(block)) {
                return buffer;
            }
        }
        return null;
    }

    private Buffer chooseUpinnedBuffer() {
        for (Buffer buffer : bufferPool) {
            if (!buffer.isPinned()) {
                return buffer;
            }
        }
        return null;
    }
}
