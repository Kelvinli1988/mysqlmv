package org.mysqlmv.etp.scanner;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.util.JdbcConstants;
import org.apache.log4j.spi.LoggerFactory;
import org.mysqlmv.common.io.db.ConnectionPool;
import org.mysqlmv.etp.mv.MaterializedView;
import org.slf4j.Logger;

import java.sql.*;
import java.util.List;

/**
 * Created by Kelvin Li on 11/21/2014 3:06 PM.
 */
public class CreateMVScanner implements Runnable {

    public static Logger logger = org.slf4j.LoggerFactory.getLogger(CreateMVScanner.class);

    @Override
    public void run() {
        try {
            runTask();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void runTask() throws SQLException {
        // 1. get connection

        // 2. get un-setup materialized view
        // 3. parse the mv definition, and create relative exprs
        // 4. create the deta table and relative intermediate tables
        // 5.
//        Connection conn = ConnectionPool.getConnection();
        String url="jdbc:mysql://localhost:3306/mysqlmv?user=root&password=123456";
        Connection conn = DriverManager.getConnection(url);
        if(conn == null) {
            logger.error("Fail to get data source");
            return;
        }
        Statement stmt = conn.createStatement();
        String findMVSql = "select * from mview where mview_setup_finished = 0 and mview_name is not null";
        stmt.execute(findMVSql);
        ResultSet rs = stmt.getResultSet();
        while(rs.next()) {
            MaterializedView mv = new MaterializedView();
            mv.setName(rs.getString("mview_name"));
            mv.setOriginalSchema(rs.getString("mview_schema"));
            mv.setDefStr(rs.getString("mview_definition"));
            initializeMV(mv);
        }
    }

    private void initializeMV(MaterializedView mv) {
        List<SQLStatement> stmtList = SQLUtils.parseStatements(mv.getDefStr(), JdbcConstants.MYSQL);
        if(stmtList.size() != 1) {
            logger.error("Invalid materialized view definition!");
            logger.error(mv.getOriginalSchema());
            logger.error(mv.getName());
            logger.error(mv.getDefStr());
            return;
        }
        mv.setDefObj(stmtList.get(0));

    }
}
