package simpledb.log;

import simpledb.file.Block;
import simpledb.file.FileMgr;
import simpledb.file.Page;

import java.io.IOException;
import java.util.Iterator;

class LogIterator implements Iterator<byte[]> {
    private final FileMgr fileMgr;
    private Block block;
    private final Page page;
    private int currentPosition;

    public LogIterator(FileMgr fileMgr, Block block) throws IOException {
        this.fileMgr = fileMgr;
        this.block = block;
        byte[] bytes = new byte[fileMgr.getBlocksize()];
        page = new Page(bytes);
        moveToBlock(block);
    }

    @Override
    public boolean hasNext() {
        return (currentPosition < fileMgr.getBlocksize()) || (block.getBlockNumber() > 0);
    }

    @Override
    public byte[] next() {
        if (currentPosition == fileMgr.getBlocksize()) {
            block = new Block(block.getFilename(), block.getBlockNumber() - 1);
            try {
                moveToBlock(block);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        byte[] record = page.getBytes(currentPosition);
        currentPosition += (Integer.BYTES + record.length);
        return record;
    }

    private void moveToBlock(Block block) throws IOException {
        fileMgr.read(block, page);
        currentPosition = page.getInt(0);
    }
}
