package db.tx.recovery;

import db.file.Page;
import db.log.LogManager;
import db.tx.Transaction;

public class StartRecord implements LogRecord {
    private final int txNum;

    public StartRecord(Page page) {
        int transactionPosition = Integer.BYTES;
        this.txNum = page.getInt(transactionPosition);
    }

    @Override
    public RecordType getRecordType() {
        return RecordType.START;
    }

    @Override
    public int getTxNumber() {
        return txNum;
    }

    @Override
    public void undo(Transaction transaction) {
    }

    public static int writeToLog(LogManager logManager, int txNum) {
        int transactionPosition = Integer.BYTES;
        int recordLen = transactionPosition + Integer.BYTES;

        byte[] record = new byte[recordLen];
        Page page = new Page(record);
        page.setInt(0, RecordType.START.ordinal());
        page.setInt(transactionPosition, txNum);

        return logManager.append(record);
    }
}
