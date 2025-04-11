package db.file;

public record FileStatistics(int blockWritten, int blockRead) {

    @Override
    public String toString() {
        return "Total blocks accessed " + (blockRead + blockWritten);
    }
}
