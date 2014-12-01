package org.mysqlmv.etp.scanner;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.util.JdbcConstants;
import org.mysqlmv.Switch;
import org.mysqlmv.common.io.db.ConnectionUtil;
import org.mysqlmv.etp.mv.MaterializedView;
import org.mysqlmv.etp.mv.MviewSetupVisitor;
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
            Switch aswitch = Switch.getSwitch();
            while(aswitch.getStatus()) {
                runTask();
//                Thread.sleep(100L);
            }
        } catch (SQLException e) {
            e.printStackTrace();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
        }
    }

    private void runTask() throws SQLException {
        // 1. get connection
        // 2. get un-setup materialized view
        // 3. parse the mv definition, and create relative exprs
        // 4. create the deta table and relative intermediate tables
        // 5.
        Connection conn = ConnectionUtil.getConnection();
        if (conn == null) {
            logger.error("Fail to get data source");
            return;
        }
        Statement stmt = conn.createStatement();
        String findMVSql = "select * from mview where mview_setup_finished = 0 and mview_name is not null and mview_id = 1";
        stmt.execute(findMVSql);
        ResultSet rs = stmt.getResultSet();
        while (rs.next()) {
            MaterializedView mv = new MaterializedView();
            mv.setName(rs.getString("mview_name"));
            mv.setOriginalSchema(rs.getString("mview_schema"));
            mv.setDefStr(rs.getString("mview_definition"));
            mv.setId(rs.getInt("mview_id"));
            initializeMV(mv);
        }
        setupTOI();
        ConnectionUtil.releaseConnection();
    }

    private void initializeMV(MaterializedView mv) throws SQLException {
        logger.info("Find materialized view to setup, id:" + mv.getId() + ", schema:" + mv.getOriginalSchema() + ", name:" + mv.getName() + ", " +
                "definition: " + mv.getDefStr());
        List<SQLStatement> stmtList = SQLUtils.parseStatements(mv.getDefStr(), JdbcConstants.MYSQL);
        if (stmtList.size() != 1) {
            logger.error("Invalid materialized view definition!");
            logger.error(mv.getOriginalSchema());
            logger.error(mv.getName());
            logger.error(mv.getDefStr());
            return;
        }
        MviewSetupVisitor visitor = new MviewSetupVisitor();
        mv.setDefObj(stmtList.get(0));
        MVContext context = new MVContext();
        context.setMview(mv);
        visitor.setContext(context);
        visitor.visit((MySqlSelectQueryBlock) ((((SQLSelectStatement) stmtList.get(0)).getSelect()).getQuery()));
        updateMVDef(mv);
    }

    private void updateMVDef(MaterializedView mv) throws SQLException {
        logger.info("Update mview definition, mview_id:" + mv.getId());
        String sql = "update mview set mview_setup_finished = 1 where mview_id = ?";
        PreparedStatement pstmt = ConnectionUtil.getConnection().prepareStatement(sql);
        pstmt.setInt(1, mv.getId());
        pstmt.execute();
        pstmt.close();
    }

    private void setupTOI() throws SQLException {
        String sql = "select * from `information_schema`.`tables` where table_schema = 'mysqlmv' and table_name = ?";
        String getUnSetupTOISql = "select distinct schema_name, table_name from mview_toi where setup_finished = 0";
        Statement getUnSetupTOIStmt = ConnectionUtil.getConnection().createStatement();
        getUnSetupTOIStmt.execute(getUnSetupTOISql);
        ResultSet rs = getUnSetupTOIStmt.getResultSet();
        PreparedStatement pstmt = ConnectionUtil.getConnection().prepareStatement(sql);

        while(rs.next()) {
            String schemaName = rs.getString("schema_name");
            String tableName = rs.getString("table_name");
            pstmt.setString(1, getTOITableName(schemaName, tableName));
            pstmt.execute();
            boolean alreadyExists = pstmt.getResultSet().next();
            if(!alreadyExists) {
                // create the table
                createTOI(schemaName, tableName);
            }
            // update the setup_finished field
            updateTOISetup(schemaName, tableName);
        }
    }

    private String getTOITableName(String schema, String table) {
//        String pattern = "cd_log_%s_%s";
        return String.format(MysqlMVConstant.TABLE_NAME_FORMAT, schema, table);
    }

    private void createTOI(String schema, String table) throws SQLException {
        logger.info("create cd_log table for schema:" + schema + ", table:" + table);
        String createTOISql = String.format(MysqlMVConstant.CREATE_TOI_TEMPLATE, schema, table);
        logger.debug(createTOISql);
        Statement stmt = ConnectionUtil.getConnection().createStatement();
        stmt.execute(createTOISql);
        stmt.close();
    }

    private void updateTOISetup(String schema, String table) throws SQLException {
        String sql = "update mview_toi set setup_finished = 1 where schema_name = ? and table_name = ? and setup_finished = 0";
        PreparedStatement pstmt = ConnectionUtil.getConnection().prepareStatement(sql);
        pstmt.setString(1, schema);
        pstmt.setString(2, table);
        pstmt.executeUpdate();
        pstmt.close();
    }
}