package db.query;

import db.record.RID;

public interface UpdateScan extends Scan{
    public void setInt(String fieldName, int value);
    public void setString(String fieldName, String value);
    public void setValue(String fieldName, Constant value);
    public void insert();
    public void delete();

    public RID getRID();
    public void moveToRID(RID rid);
}
