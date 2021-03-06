package org.mysqlmv.common.util.db;


import com.alibaba.druid.pool.DruidDataSource;
import org.mysqlmv.common.config.reader.ConfigFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by Kelvin Li on 11/21/2014 3:14 PM.
 */
public class ConnectionPool {
    private static DruidDataSource ds;
    static {
        ds = new DruidDataSource();
        ConfigFactory cfg = ConfigFactory.getINSTANCE();
        System.out.println(cfg.getProperty("jdbc.url"));
        ds.setUrl(cfg.getProperty("jdbc.url"));
//        ds.setJdbcUrl();
        System.out.println(cfg.getProperty("username"));
        ds.setUsername(cfg.getProperty("username"));
        System.out.println(cfg.getProperty("password"));
        ds.setPassword(cfg.getProperty("password"));
        System.out.println(cfg.getProperty("initcount"));
        ds.setInitialSize(Integer.parseInt(cfg.getProperty("initcount")));
        System.out.println(cfg.getProperty("maxcount"));
        ds.setMaxActive(Integer.parseInt(cfg.getProperty("maxcount")));
    }

    private ConnectionPool() {
    }

    public static Connection getConnection() throws SQLException {
//        return ds.getConnection();
        String url="jdbc:mysql://localhost:3306/mysqlmv?user=root&password=123456";
        return DriverManager.getConnection(url);
    }

    public static void releaseConnection() {
//        ds.
    }
}
