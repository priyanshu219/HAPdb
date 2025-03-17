package db.tx.recovery;

import db.file.Page;
import db.log.LogManager;
import db.tx.Transaction;

public class CheckpointRecord implements LogRecord {

    public CheckpointRecord(Page page) {
    }

    @Override
    public RecordType getRecordType() {
        return RecordType.CHECKPOINT;
    }

    @Override
    public int getTxNumber() {
        return 0;
    }

    @Override
    public void undo(Transaction transaction) {
    }

    public static int writeToLog(LogManager logManager) {
        int recordLen = Integer.BYTES;

        byte[] record = new byte[recordLen];
        Page page = new Page(record);
        page.setInt(0, RecordType.CHECKPOINT.ordinal());

        return logManager.append(record);
    }
}
