package org.mysqlmv.common.io.db;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.mysqlmv.common.config.reader.ConfigFactory;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by Kelvin Li on 11/21/2014 3:14 PM.
 */
public class ConnectionPool {
    private static ComboPooledDataSource ds;
    static {
        ds = new ComboPooledDataSource();
    }

    private ConnectionPool() {
        ConfigFactory cfg = ConfigFactory.getINSTANCE();
        ds.setJdbcUrl(cfg.getProperty("jdbc.url"));
        ds.setUser(cfg.getProperty("username"));
        ds.setPassword(cfg.getProperty("password"));
        ds.setInitialPoolSize(Integer.parseInt(cfg.getProperty("initcount")));
        ds.setMaxPoolSize(Integer.parseInt(cfg.getProperty("maxcount")));
    }

    public static Connection getConnection() throws SQLException {
        return ds.getConnection();
    }
}
