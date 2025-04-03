package db.log;

import db.file.Block;
import db.file.FileManager;
import db.file.Page;

import java.util.Iterator;

class LogIterator implements Iterator<byte[]> {
    private final FileManager fileManager;
    private final Page page;
    private Block block;
    private int currentPosition;

    public LogIterator(FileManager fileManager, Block block) {
        this.fileManager = fileManager;
        this.block = block;
        byte[] bytes = new byte[fileManager.getBlockSize()];
        page = new Page(bytes);
        moveToBlock(block);
    }

    @Override
    public boolean hasNext() {
        return (currentPosition < fileManager.getBlockSize()) || (block.blockNumber() > 0);
    }

    @Override
    public byte[] next() {
        if (currentPosition == fileManager.getBlockSize()) {
            block = new Block(block.fileName(), block.blockNumber() - 1);
            moveToBlock(block);
        }
        byte[] record = page.getBytes(currentPosition);
        currentPosition += (Integer.BYTES + record.length);
        return record;
    }

    private void moveToBlock(Block block) {
        fileManager.read(block, page);
        currentPosition = page.getInt(0);
    }
}
