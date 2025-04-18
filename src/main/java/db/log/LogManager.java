package db.log;

import db.file.Block;
import db.file.FileManager;
import db.file.Page;

import java.util.Iterator;

public class LogManager {
    private final FileManager fileManager;
    private final String logFile;
    private final Page logPage;
    private Block currentBlock;
    private int latestLSN;
    private int lastSavedLSN;

    public LogManager(FileManager fileManager, String logFile) {
        this.fileManager = fileManager;
        this.logFile = logFile;
        this.latestLSN = 0;
        this.lastSavedLSN = 0;
        byte[] bytes = new byte[FileManager.getBlockSize()];
        this.logPage = new Page(bytes);
        int logSize = fileManager.length(logFile);
        if (logSize == 0) {
            currentBlock = appendNewBlock();
        } else {
            currentBlock = new Block(logFile, logSize - 1);
            fileManager.read(currentBlock, logPage);
        }
    }

    public void flush(int lsn) {
        if (lsn >= lastSavedLSN) {
            flush();
        }
    }

    public Iterator<byte[]> iterator() {
        flush();
        return new LogIterator(fileManager, currentBlock);
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

        //using setBytes we are appending length of the record
        // logRecord: Integer.Bytes(string length) + stringValue.length() + Integer.Bytes(intValue)
        logPage.setBytes(recordPosition, logRecord);

        //setting the offset of last record at 0 offset of logPage
        logPage.setInt(0, recordPosition);
        latestLSN += 1;
        return latestLSN;
    }

    private Block appendNewBlock() {
        Block block = fileManager.append(logFile);
        logPage.setInt(0, FileManager.getBlockSize());
        fileManager.write(block, logPage);
        return block;
    }

    private void flush() {
        fileManager.write(currentBlock, logPage);
        lastSavedLSN = latestLSN;
    }
}
