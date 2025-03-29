package db.query;

public interface Scan {
    void beforeFirst();

    boolean next();

    int getInt(String fieldName);

    String getString(String fieldName);

    Constant getValue(String fieldName);

    boolean hasField(String fieldName);

    void close();
}
