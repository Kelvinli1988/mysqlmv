package org.mysqlmv.common.util.db;


import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by Kelvin Li on 11/24/2014 2:50 PM.
 */
public class ConnectionUtil {
    private static ThreadLocal<Connection> conn = new ThreadLocal<Connection>();

    public static Connection getConnection() throws SQLException {
        if(conn.get() == null) {
            conn.set(ConnectionPool.getConnection());
        }
        return conn.get();
    }

    public static void releaseConnection() {
        conn.remove();
    }
}
