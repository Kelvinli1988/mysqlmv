package org.mysqlmv.common.conn;

import org.mysqlmv.common.config.reader.ConfigFactory;
import org.mysqlmv.common.exception.CDCException;
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Manage the connection.
 * Created by Kelvin Li on 6/1/2015.
 */
public class ConnectionManager {

    public static Logger logger = org.slf4j.LoggerFactory.getLogger(ConnectionManager.class);
    /**
     *
     */
    private static Properties connProp;

    public static String DRIVER = "com.mysql.jdbc.Driver";

    private static final String DEFAULT_DB = "mysqlmv";

    private static ConnectionManager INSTANCE = new ConnectionManager();

    private ConnectionManager() {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            logger.error("Driver class not found.", e);
        }
        connProp = new Properties();
        ConfigFactory factory = ConfigFactory.getINSTANCE();
        connProp.put("host", factory.getProperty("host"));
        connProp.put("port", factory.getProperty("port"));
        connProp.put("user", factory.getProperty("user"));
        connProp.put("password", factory.getProperty("password"));
    }

    public static ConnectionManager getInstance() {
        return INSTANCE;
    }

    /**
     * Get connection to specified database.
     * @param database
     * @return connection
     * @throws CDCException
     */
    public Connection getConnection(String database) throws CDCException {
        Connection con = null;
        String url = "jdbc:mysql://" + connProp.getProperty("host") + ":" +
                connProp.getProperty("port") + "/" + database;
        try {
            con = DriverManager.getConnection(url, connProp);
        } catch (SQLException ex) {
            logger.error("Fail to create data  base connection on database :" + database, ex);
            throw new CDCException(ex);
        }
        return con;
    }

    /**
     * Get connection to mysqlmv db.
     * @return
     * @throws CDCException
     */
    public Connection getDefaultConnection() throws CDCException {
        return getConnection(DEFAULT_DB);
    }

    /**
     * Close specified connection.
     * @param conn
     * @throws CDCException
     */
    public void releaseConnection(Connection conn) throws CDCException {
        try {
            conn.close();
        } catch (SQLException e) {
            logger.error("Fail to release connection.", e);
            throw new CDCException(e);
        }
    }

}
