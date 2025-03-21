package db.file;

public record Block(String fileName, int blockNumber) {

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Block block)) {
            return false;
        }
        return fileName.equals(block.fileName()) && blockNumber == block.blockNumber();
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
