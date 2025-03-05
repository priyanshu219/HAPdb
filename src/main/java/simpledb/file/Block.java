package simpledb.file;

public class Block {
    private final String filename;
    private final int blockNumber;

    public Block(String filename, int blockNumber) {
        this.filename = filename;
        this.blockNumber = blockNumber;
    }

    public String getFilename() {
        return filename;
    }

    public int getBlockNumber() {
        return blockNumber;
    }

    @Override
    public boolean equals(Object obj) {
        Block blk = (Block) obj;
        return filename.equals(blk.getFilename()) && blockNumber == blk.getBlockNumber();
    }

    @Override
    public String toString() {
        return "[file " + filename + ", block " + blockNumber + "]";
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }
}
