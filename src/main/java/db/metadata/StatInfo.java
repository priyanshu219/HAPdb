package db.metadata;

public record StatInfo(int numBlocks, int numRecords) {
    public int getDistinctValues(String fieldName) {
        return 1 + (numRecords / 3);
    }
}
