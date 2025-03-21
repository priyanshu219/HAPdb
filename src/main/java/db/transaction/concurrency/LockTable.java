package db.transaction.concurrency;

import db.file.Block;

import java.util.Map;
import java.util.HashMap;

public class LockTable {
    private static final long MAX_TIME = 10000;
    private final Map<Block, Integer> locks;

    public LockTable() {
        this.locks = new HashMap<>();
    }

    public synchronized void sLock(Block block) throws LockAbortException{
        try {
            long timestamp = System.currentTimeMillis();
            while (hasXLock(block) && !waitingTooLong(timestamp)) {
                wait(MAX_TIME);
            }
            if (hasXLock(block)) {
                throw new LockAbortException();
            }
            int val = getLockVal(block);
            locks.put(block, val + 1);
        } catch (InterruptedException ex) {
            throw new LockAbortException();
        }
    }

    public synchronized void xLock(Block block) throws LockAbortException{
        try {
            long timestamp = System.currentTimeMillis();
            while (hasOtherSLocks(block) && !waitingTooLong(timestamp)) {
                wait(MAX_TIME);
            }
            if (hasOtherSLocks(block)) {
                throw new LockAbortException();
            }
            locks.put(block, -1);
        } catch (InterruptedException ex) {
            throw new LockAbortException();
        }
    }

    public synchronized void unlock(Block block) {
        int val = getLockVal(block);
        if (val > 1) {
            locks.put(block, val - 1);
        } else {
            locks.remove(block);
            notifyAll();
        }
    }

    private boolean hasXLock(Block block) {
        return getLockVal(block) < 0;
    }

    private boolean hasOtherSLocks(Block block) {
        return getLockVal(block) > 1;
    }

    private int getLockVal(Block block) {
        Integer iVal = locks.get(block);
        return (iVal == null) ? 0 : iVal;
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean waitingTooLong(long startTime) {
        return (System.currentTimeMillis() - startTime) > MAX_TIME;
    }
}
