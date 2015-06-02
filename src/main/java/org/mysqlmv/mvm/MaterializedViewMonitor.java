package org.mysqlmv.mvm;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.util.JdbcConstants;
import org.mysqlmv.common.conn.ConnectionManager;
import org.mysqlmv.common.exception.CDCException;
import org.mysqlmv.mvm.mv.BaseDeltaTable;
import org.mysqlmv.mvm.mv.MVContext;
import org.mysqlmv.mvm.mv.MaterializedView;
import org.mysqlmv.mvm.mv.MviewMonitorVisitor;
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * 1. find uninitialized materialized view.
 * 2. parse the view definition.
 * 3. visit the sql tree to find base table.
 * 4. create base table delta table.
 * 5. update the materialized view status.
 * 6. issue mark command.
 * -- another loop.
 *
 * Created by Kelvin Li on 6/1/2015.
 */
public class MaterializedViewMonitor implements Runnable {

    public static Logger logger = org.slf4j.LoggerFactory.getLogger(MaterializedViewMonitor.class);

    private Connection dbCon;

    public MaterializedViewMonitor() throws CDCException {
        dbCon = ConnectionManager.getInstance().getDefaultConnection();
    }

    @Override
    public void run() {
        while(true) {

        }
    }

    public List<MaterializedView> findUnintializedMV() throws CDCException {
        List<MaterializedView> uninitializedMVList = new ArrayList<MaterializedView>();
        String sql = "select * from mview where mview_setup_finished = 0 and mview_name is not null";
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            pstmt = dbCon.prepareStatement(sql);
            pstmt.execute();
            rs = pstmt.getResultSet();
            while(rs.next()) {
                MaterializedView mv = new MaterializedView();
                mv.setName(rs.getString("mview_name"));
                mv.setOriginalSchema(rs.getString("mview_schema"));
                mv.setDefStr(rs.getString("mview_definition"));
                mv.setId(rs.getInt("mview_id"));
                uninitializedMVList.add(mv);
            }
        } catch (SQLException e) {
            logger.error("Error while finding uninitalized materialized view.", e);
        } finally {
            try {
                pstmt.close();
                rs.close();
            } catch (Exception ignore) {
            }
        }
        return uninitializedMVList;
    }

    public List<BaseDeltaTable> initializeMV(MaterializedView mv) {
        logger.info("Find materialized view to setup, id:" + mv.getId() + ", schema:" + mv.getOriginalSchema() + ", name:" + mv.getName() + ", " +
                "definition: " + mv.getDefStr());
        List<SQLStatement> stmtList = SQLUtils.parseStatements(mv.getDefStr(), JdbcConstants.MYSQL);
        if (stmtList.size() != 1) {
            logger.error("Invalid materialized view definition!");
            logger.error(mv.getOriginalSchema());
            logger.error(mv.getName());
            logger.error(mv.getDefStr());
            return null;
        }
        MviewMonitorVisitor visitor = new MviewMonitorVisitor();
        mv.setDefObj(stmtList.get(0));
        MVContext context = new MVContext();
        context.setMview(mv);
        visitor.setContext(context);
        visitor.visit((MySqlSelectQueryBlock) ((((SQLSelectStatement) stmtList.get(0)).getSelect()).getQuery()));
        return context.getDeltaTableList();
    }

    public void setupDeltaTable(BaseDeltaTable delta) {

    }

    private void issueTableMarker(BaseDeltaTable delta) {

    }
}
