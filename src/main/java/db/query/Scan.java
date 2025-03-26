package db.query;

public interface Scan {
    public void beforeFirst();
    public boolean next();

    public int getInt(String fieldName);
    public String getString(String fieldName);
    public Constant getValue(String fieldName);

    public boolean hasField(String fieldName);

    public void close();
}
