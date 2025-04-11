package db.index;

import db.query.Constant;
import db.record.RID;

public interface Index {
    void beforeFirst(Constant searchKey);

    boolean next();

    RID getDataRid();

    void insert(Constant dataVal, RID dataRID);

    void delete(Constant dataVal, RID dataRID);

    void close();
}
