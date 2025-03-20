package db.file;

public class Block {
    private final String fileName;
    private final int blockNumber;

    public Block(String fileName, int blockNumber) {
        this.fileName = fileName;
        this.blockNumber = blockNumber;
    }

    public String getFileName() {
        return fileName;
    }

    public int getBlockNumber() {
        return blockNumber;
    }

    @Override
    public boolean equals(Object obj) {
        Block blk = (Block) obj;
        return fileName.equals(blk.getFileName()) && blockNumber == blk.getBlockNumber();
    }

    @Override
    public String toString() {
        return "[file " + fileName + ", block " + blockNumber + "]";
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }
}
