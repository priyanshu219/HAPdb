package db.log;

import db.file.Block;
import db.file.FileManager;
import db.file.Page;

import java.io.IOException;
import java.util.Iterator;

class LogIterator implements Iterator<byte[]> {
    private final FileManager fileManager;
    private Block block;
    private final Page page;
    private int currentPosition;

    public LogIterator(FileManager fileManager, Block block) throws IOException {
        this.fileManager = fileManager;
        this.block = block;
        byte[] bytes = new byte[fileManager.getBlocksize()];
        page = new Page(bytes);
        moveToBlock(block);
    }

    @Override
    public boolean hasNext() {
        return (currentPosition < fileManager.getBlocksize()) || (block.getBlockNumber() > 0);
    }

    @Override
    public byte[] next() {
        if (currentPosition == fileManager.getBlocksize()) {
            block = new Block(block.getFileName(), block.getBlockNumber() - 1);
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
        fileManager.read(block, page);
        currentPosition = page.getInt(0);
    }
}
