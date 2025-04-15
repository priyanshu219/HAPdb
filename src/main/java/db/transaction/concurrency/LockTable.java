package db.transaction.concurrency;

import db.file.Block;

import java.util.*;

public class LockTable {
    private static final long MAX_TIME = 10000;
    private final Map<Block, List<Integer>> sLocks;
    private final Map<Block, List<Integer>> xLocks;

    private final Map<Block, Object> sLatch;
    private final Map<Block, Object> xLatch;

    public LockTable() {
        this.sLocks = new HashMap<>();
        this.xLocks = new HashMap<>();

        this.sLatch = new HashMap<>();
        this.xLatch = new HashMap<>();
    }

    public synchronized void sLock(Block block, int txNum) throws LockAbortException {
        try {
            long timestamp = System.currentTimeMillis();

            boolean shouldDie = xLocks.get(block).stream().anyMatch(id -> id < txNum);
            if (shouldDie) {
                throw new LockAbortException();
            }

            Object lock = sLatch.computeIfAbsent(block, key -> new Object());

            while (hasXLock(block) && !waitingTooLong(timestamp)) {
                lock.wait(MAX_TIME);
            }
            if (hasXLock(block)) {
                throw new LockAbortException();
            }
            sLocks.computeIfAbsent(block, key -> new ArrayList<>()).add(txNum);
        } catch (InterruptedException ex) {
            throw new LockAbortException();
        }
    }

    public synchronized void xLock(Block block, int txNum) throws LockAbortException {
        try {
            long timestamp = System.currentTimeMillis();

            boolean shouldDie = sLocks.get(block).stream().anyMatch(id -> id < txNum);
            if (shouldDie) {
                throw new LockAbortException();
            }

            Object lock = xLatch.computeIfAbsent(block, key -> new Object());
            while (hasOtherSLocks(block) && !waitingTooLong(timestamp)) {
                lock.wait(MAX_TIME);
            }

            if (hasOtherSLocks(block)) {
                throw new LockAbortException();
            }
            xLocks.computeIfAbsent(block, key -> new ArrayList<>()).add(txNum);
        } catch (InterruptedException ex) {
            throw new LockAbortException();
        }
    }

    public synchronized void unlock(Block block, int txNum) {
        if (sLocks.get(block).remove((Object) txNum)) {
            if (sLocks.get(block).isEmpty()) {
                sLocks.remove(block);
            }
            sLatch.get(block).notifyAll();
        }

        if (xLocks.get(block).remove((Object) txNum)) {
            if (xLocks.get(block).isEmpty()) {
                xLocks.remove(block);
            }
            xLatch.get(block).notifyAll();
        }
    }

    private boolean hasXLock(Block block) {
        return !xLocks.get(block).isEmpty();
    }

    private boolean hasOtherSLocks(Block block) {
        return sLocks.get(block).size() > 1;
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean waitingTooLong(long startTime) {
        return (System.currentTimeMillis() - startTime) > MAX_TIME;
    }
}
