package org.mysqlmv.etp.scanner;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.util.JdbcConstants;
import org.mysqlmv.common.io.db.ConnectionUtil;
import org.mysqlmv.common.io.db.QueryCallBack;
import org.mysqlmv.common.util.CollectionUtils;
import org.mysqlmv.common.util.db.DBUtil;
import org.mysqlmv.etp.context.ToiContext;
import org.mysqlmv.etp.context.ToiEntry;
import org.mysqlmv.etp.context.ToiValue;
import org.mysqlmv.etp.dao.MaterializedViewDao;
import org.mysqlmv.etp.mv.MaterializedView;
import org.mysqlmv.etp.mv.MviewSetupVisitor;
import org.slf4j.Logger;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * Created by Kelvin Li on 12/5/2014 10:35 AM.
 */
public class MVInitializer {
    public static Logger logger = org.slf4j.LoggerFactory.getLogger(MVInitializer.class);

    private MaterializedView mv;

    public MVInitializer(MaterializedView mv) {
        this.mv = mv;
    }

    public void initializeMV() {
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
        updateMVDef();
        setupTOI();
    }



    private void updateMVDef() {
        logger.info("Update mview definition, mview_id:" + mv.getId());
        DBUtil.executeInPrepareStmt(new QueryCallBack() {
            @Override
            public String getSql() {
                return "update mview set mview_setup_finished = 1 where mview_id = ?";
            }

            @Override
            public Object doInCallback(PreparedStatement pstmt) throws SQLException {
                pstmt.setInt(1, mv.getId());
                pstmt.execute();
                return null;
            }
        });
    }

    private void setupTOI() {
        List<ToiEntry> entryList = MaterializedViewDao.findUnsetupToiEntry();
        if(!CollectionUtils.isEmpty(entryList)) {

        }
        String sql = "select * from `information_schema`.`tables` where table_schema = 'mysqlmv' and table_name = ?";
//        PreparedStatement pstmt = ConnectionUtil.getConnection().prepareStatement(sql);
//
//        while(rs.next()) {
//
//            pstmt.setString(1, getTOITableName(schemaName, tableName));
//            pstmt.execute();
//            boolean alreadyExists = pstmt.getResultSet().next();
//            if(!alreadyExists) {
//                // create the table
//                createTOI(schemaName, tableName);
//            }
//            // update the setup_finished field
//            updateTOISetup(schemaName, tableName);
//        }
    }

    private String getTOITableName(String schema, String table) {
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
        // Add toi setup into toi context
        ToiContext.addToiEntry(new ToiEntry(schema, table), new ToiValue(DBUtil.getLastInsertedID(), mv.getId()));
    }
}
