package db.transaction.recovery;

import db.file.Page;
import db.log.LogManager;
import db.transaction.Transaction;

public class RollbackRecord implements LogRecord {
    private final int txNum;

    public RollbackRecord(Page page) {
        int transactionPosition = Integer.BYTES;
        this.txNum = page.getInt(transactionPosition);
    }

    public static int writeToLog(LogManager logManager, int txNum) {
        int transactionPosition = Integer.BYTES;
        int recordLen = transactionPosition + Integer.BYTES;

        byte[] record = new byte[recordLen];
        Page page = new Page(record);
        page.setInt(0, RecordType.COMMIT.ordinal());
        page.setInt(transactionPosition, txNum);

        return logManager.append(record);
    }

    @Override
    public RecordType getRecordType() {
        return RecordType.ROLLBACK;
    }

    @Override
    public int getTxNumber() {
        return txNum;
    }

    @Override
    public void undo(Transaction transaction) {
    }
}
