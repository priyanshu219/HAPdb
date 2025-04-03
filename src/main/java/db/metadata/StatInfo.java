package db.metadata;

public record StatInfo(int blocksAccessed, int recordsOutput) {
    public int getDistinctValues(String fieldName) {
        return 1 + (recordsOutput / 3);
    }
}
