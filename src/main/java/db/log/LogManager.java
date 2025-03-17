package db.log;

import db.file.Block;
import db.file.FileManager;
import db.file.Page;

import java.io.IOException;
import java.util.Iterator;

public class LogMgr {
    private FileMgr fileMgr;
    private String logFile;
    private final Page logPage;
    private Block currentBlock;
    private int latestLSN;
    private int lastSavedLSN;

    public LogMgr(FileMgr fileMgr, String logFile) throws IOException {
        this.latestLSN = 0;
        this.lastSavedLSN = 0;
        byte[] b = new byte[fileMgr.getBlocksize()];
        this.logPage = new Page(b);
        int logSize = fileMgr.length(logFile);
        if (logSize == 0) {
            currentBlock = appendNewBlock();
        } else {
            currentBlock = new Block(logFile, logSize - 1);
            fileMgr.read(currentBlock, logPage);
        }
    }

    public void flush(int lsn) {
        if (lsn >= lastSavedLSN) {
            flush();
        }
    }

    public Iterator<byte[]> iterator() throws IOException {
        flush();
        return new LogIterator(fileMgr, currentBlock);
    }

    public synchronized int append(byte[] logRecord) {
        int boundary = logPage.getInt(0);
        int recordSize = logRecord.length;
        int bytesNeeded = recordSize + Integer.BYTES;
        if (boundary - bytesNeeded < Integer.BYTES) {
            flush();
            currentBlock = appendNewBlock();
            boundary = logPage.getInt(0);
        }

        // filling the page from right to left
        // in every iteration recordPosition becomes small to fulfil the condition
        int recordPosition = boundary - bytesNeeded;
        logPage.setBytes(recordPosition, logRecord);
        logPage.setInt(0, recordPosition);
        latestLSN += 1;
        return latestLSN;
    }

    private Block appendNewBlock() {
        Block block = fileMgr.append(logFile);
        logPage.setInt(0, fileMgr.getBlocksize());
        fileMgr.write(block, logPage);
        return block;
    }

    private void flush() {
        fileMgr.write(currentBlock, logPage);
        lastSavedLSN = latestLSN;
    }
}
