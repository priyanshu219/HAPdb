package db.transaction.concurrency;

import db.file.Block;

import java.util.HashMap;
import java.util.Map;

public class ConcurrencyManager {
    private static final LockTable LOCK_TABLE = new LockTable();
    private final Map<Block, String> locks;
    private final int txNum;

    public ConcurrencyManager(int txNum) {
        this.locks = new HashMap<>();
        this.txNum = txNum;
    }

    public void sLock(Block block) {
        if (locks.get(block) == null) {
            LOCK_TABLE.sLock(block, txNum);
            locks.put(block, "S");
        }
    }

    /**
     * For xLock we acquire sLock first
     */
    public void xLock(Block block) {
        if (!hasXLock(block)) {
            sLock(block);
            LOCK_TABLE.xLock(block, txNum);
            locks.put(block, "X");
        }
    }

    public void release() {
        for (Block block : locks.keySet()) {
            LOCK_TABLE.unlock(block, txNum);
        }
        locks.clear();
    }

    private boolean hasXLock(Block block) {
        String lockType = locks.get(block);
        return ((lockType != null) && (lockType.equals("X")));
    }
}
