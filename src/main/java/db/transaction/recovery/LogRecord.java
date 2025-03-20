package db.transaction.recovery;

import db.file.Page;
import db.transaction.Transaction;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;

public interface LogRecord {
    enum RecordType {
        CHECKPOINT,
        START,
        COMMIT,
        ROLLBACK,
        SETINT,
        SETSTRING;
    }

    RecordType getRecordType();
    int getTxNumber();

    /**
     * Logging only old value due to undo only recovery
     */
    void undo(Transaction transaction) throws IOException;

    Map<Integer, Function<Page, LogRecord>> RECORD_FACTORIES = Map.of(RecordType.CHECKPOINT.ordinal(), CheckpointRecord::new,
            RecordType.START.ordinal(), StartRecord::new,
            RecordType.COMMIT.ordinal(), CommitRecord::new,
            RecordType.ROLLBACK.ordinal(), RollbackRecord::new,
            RecordType.SETINT.ordinal(), SetIntRecord::new,
            RecordType.SETSTRING.ordinal(), SetStringRecord::new);

    static LogRecord createLogRecord(byte[] bytes) {
        Page page = new Page(bytes);
        return RECORD_FACTORIES.getOrDefault(page.getInt(0), null).apply(page);
    }
}