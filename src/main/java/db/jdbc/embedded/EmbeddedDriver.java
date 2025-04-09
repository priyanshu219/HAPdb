package db.jdbc.embedded;

import db.jdbc.DriverAdapter;
import db.server.HAPdb;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class EmbeddedDriver extends DriverAdapter {
    public Connection connect(String dbName, Properties properties) throws SQLException {
        HAPdb db = new HAPdb(dbName);
        return new EmbeddedConnection(db);
    }
}
