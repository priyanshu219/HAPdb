package db.buffer;

import db.file.Block;
import db.file.FileManager;
import db.log.LogManager;

import java.util.HashMap;
import java.util.Map;

public class BufferManager {
    private static final long MAX_TIME = 10000;
    private final Buffer[] bufferPool;
    private final Map<Block, Buffer> bufferMap;
    private final LRUReplacement lruReplacement;
    private int totalAvailable;

    public BufferManager(FileManager fileManager, LogManager logManager, int totalBuffers) {
        this.bufferPool = new Buffer[totalBuffers];
        this.totalAvailable = totalBuffers;
        for (int i = 0; i < totalBuffers; i++) {
            bufferPool[i] = new Buffer(fileManager, logManager);
        }
        this.bufferMap = new HashMap<>();
        this.lruReplacement = new LRUReplacement();
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
            lruReplacement.insert(buffer);
            totalAvailable++;
            notifyAll();
        }
    }

    public synchronized Buffer pin(Block block) throws BufferAbortException {
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

    private Buffer tryToPin(Block block) {
        Buffer buffer = findExistingBuffer(block);
        if (null == buffer) {
            BufferStats.incrementCacheMiss();

            buffer = chooseUpinnedBuffer();
            if (null == buffer) {
                return null;
            }
            bufferMap.remove(buffer.getBlock());
            buffer.assignToBlock(block);
        }
        if (!buffer.isPinned()) {
            totalAvailable--;
        }
        buffer.pin();
        bufferMap.put(buffer.getBlock(), buffer);
        return buffer;
    }

    private Buffer findExistingBuffer(Block block) {
        if (bufferMap.containsKey(block)) {
            Buffer buffer = bufferMap.get(block);
            lruReplacement.remove(buffer);

            BufferStats.incrementCacheHit();
            return buffer;
        }
        return null;
    }

    private Buffer chooseUpinnedBuffer() {
        Buffer buffer = lruReplacement.get();

        if (null != buffer) {
            lruReplacement.remove(buffer);
        }

        return buffer;
    }
}
