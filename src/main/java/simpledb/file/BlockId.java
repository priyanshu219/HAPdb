package simpledb.file;

public class BlockId {
    private final String filename;
    private final int blknum;

    BlockId(String filename, int blknum) {
        this.filename = filename;
        this.blknum = blknum;
    }

    public String getFilename() {
        return filename;
    }

    public int getBlknum() {
        return blknum;
    }

    @Override
    public boolean equals(Object obj) {
        BlockId blk = (BlockId) obj;
        return filename.equals(blk.getFilename()) && blknum == blk.getBlknum();
    }

    @Override
    public String toString() {
        return "[file " + filename + ", block " + blknum + "]";
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }
}
