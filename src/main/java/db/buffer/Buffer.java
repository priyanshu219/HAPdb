package db.buffer;

import db.file.Block;
import db.file.FileManager;
import db.file.Page;
import db.log.LogManager;

public class Buffer {
    private final FileManager fileManager;
    private final LogManager logManager;
    private final Page contents;
    private Block block;
    private int pins;
    private int txnnum;
    private int lsn;

    public Buffer(FileManager fileManager, LogManager logManager) {
        this.fileManager = fileManager;
        this.logManager = logManager;
        contents = new Page(fileManager.getBlockSize());
        this.block = null;
        this.pins = 0;
        this.txnnum = -1;
        this.lsn = -1;
    }

    public Page getContents() {
        return this.contents;
    }

    public Block getBlock() {
        return this.block;
    }

    public void setModified(int txnnum, int lsn) {
        this.txnnum = txnnum;
        if (lsn >= 0) {
            this.lsn = lsn;
        }
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean isPinned() {
        return this.pins > 0;
    }

    public int modifyingTx() {
        return this.txnnum;
    }

    void assignToBlock(Block block) {
        flush();
        this.block = block;
        fileManager.read(block, contents);
        pins = 0;
    }

    void flush() {
        if (this.txnnum >= 0) {
            logManager.flush(lsn);
            fileManager.write(block, contents);
            txnnum = -1;
        }
    }

    void pin() {
        pins++;
    }

    void unpin() {
        pins--;
    }
}
