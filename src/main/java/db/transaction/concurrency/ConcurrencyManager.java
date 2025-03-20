package db.transaction.concurrency;

import db.file.Block;

import java.util.HashMap;
import java.util.Map;

public class ConcurrencyManager {
    private static final LockTable LOCK_TABLE = new LockTable();
    private final Map<Block, String> locks;

    public ConcurrencyManager() {
        this.locks = new HashMap<>();
    }

    public void sLock(Block block) {
        if (locks.get(block) == null) {
            LOCK_TABLE.sLock(block);
            locks.put(block, "S");
        }
    }
    /**
     * For xLock we acquire sLock first
     */
    public void xLock(Block block) {
        if (!hasXLock(block)) {
            sLock(block);
            LOCK_TABLE.xLock(block);
            locks.put(block, "X");
        }
    }

    public void release() {
        for (Block block : locks.keySet()) {
            LOCK_TABLE.unlock(block);
        }
        locks.clear();
    }

    private boolean hasXLock(Block block) {
        String lockType = locks.get(block);
        return ((lockType != null) && (lockType.equals("X")));
    }
}
