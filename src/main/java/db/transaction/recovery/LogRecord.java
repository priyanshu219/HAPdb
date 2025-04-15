package db.transaction.recovery;

import db.file.Page;
import db.transaction.Transaction;

public interface LogRecord {

    static LogRecord createLogRecord(byte[] bytes) {
        Page page = new Page(bytes);

        return switch (page.getInt(0)) {
            case 0 -> new CheckpointRecord(page);
            case 1 -> new StartRecord(page);
            case 2 -> new CommitRecord(page);
            case 3 -> new RollbackRecord(page);
            case 4 -> new SetIntRecord(page);
            case 5 -> new SetStringRecord(page);
            case 6 -> new BlockUpdateRecord(page);
            case 7 -> new AppendBlockRecord(page);
            default -> throw new IllegalStateException("Unexpected value: " + page.getInt(0));
        };
    }

    RecordType getRecordType();

    int getTxNumber();

    /**
     * Logging only old value due to undo only recovery
     */
    void undo(Transaction transaction);

    enum RecordType {
        CHECKPOINT,
        START,
        COMMIT,
        ROLLBACK,
        SETINT,
        SETSTRING,
        BLOCK_UPDATE,
        APPEND_BLOCK
    }
}