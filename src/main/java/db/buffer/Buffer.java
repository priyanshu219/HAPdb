package db.buffer;

import db.file.Block;
import db.file.FileManager;
import db.file.Page;
import db.log.LogManager;

import java.io.IOException;

public class Buffer {
    private final FileMgr fileMgr;
    private final LogMgr logMgr;
    private final Page contents;
    private Block block;
    private int pins;
    private int txnnum;
    private int lsn;

    public Buffer(FileMgr fileMgr, LogMgr logMgr) {
        this.fileMgr = fileMgr;
        this.logMgr = logMgr;
        contents = new Page(fileMgr.getBlocksize());
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

    public boolean isPinned() {
        return this.pins > 0;
    }

    public int modifyingTx() {
        return this.txnnum;
    }

    void assignToBlock(Block block) throws IOException {
        flush();
        this.block = block;
        fileMgr.read(block, contents);
        pins = 0;
    }

    void flush() {
        if (this.txnnum >= 0) {
            logMgr.flush(lsn);
            fileMgr.write(block, contents);
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
