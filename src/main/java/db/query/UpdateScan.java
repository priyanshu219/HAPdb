package db.query;

import db.record.RID;

public interface UpdateScan extends Scan {
    void setInt(String fieldName, int value);

    void setString(String fieldName, String value);

    void setValue(String fieldName, Constant value);

    void insert();

    void delete();

    RID getRID();

    void moveToRID(RID rid);
}
