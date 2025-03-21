package db.file;

public record Block(String fileName, int blockNumber) {

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Block(String name, int number))) {
            return false;
        }
        return fileName.equals(name) && blockNumber == number;
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
