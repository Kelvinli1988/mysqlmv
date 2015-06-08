package org.mysqlmv.mvm;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.util.JdbcConstants;
import org.mysqlmv.common.conn.ConnectionManager;
import org.mysqlmv.common.exception.CDCException;
import org.mysqlmv.mvm.mv.*;
import org.mysqlmv.mvm.sql.transaction.GenerateMVExpressionTransaction;
import org.mysqlmv.mvm.sql.transaction.MVInitliazeGlobalTransaction;
import org.mysqlmv.mvm.sql.transaction.SetupDeltaTableTransaction;
import org.mysqlmv.mvm.sql.transaction.TableMarkerTransaction;
import org.mysqlmv.mvm.sql.transaction.exception.TransactionException;
import org.slf4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 1. find uninitialized materialized view.
 * 2. parse the view definition.
 * 3. visit the sql tree to find base table.
 * 4. create base table delta table.
 * 5. generate materialized view expressions.
 * 6. issue mark command.
 * 7. update the materialized view status.
 * -- another loop.
 * <p/>
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
        while (true) {
            scan();
            try {
                Thread.sleep(30 * 1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void scan() {
        try {
            List<MaterializedView> mvList = findUnintializedMV();
            for(MaterializedView mv : mvList) {
                initializeMV(mv);
            }
        } catch (CDCException e) {
            e.printStackTrace();
        }
    }

    public List<MaterializedView> findUnintializedMV() throws CDCException {
        List<MaterializedView> uninitializedMVList = new ArrayList<MaterializedView>();
        String sql = "select * from mview where mview_status = 0 and mview_name is not null";
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            pstmt = dbCon.prepareStatement(sql);
            pstmt.execute();
            rs = pstmt.getResultSet();
            while (rs.next()) {
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

    public void initializeMV(MaterializedView mv) {
        logger.info("Find materialized view to setup, id:" + mv.getId() + ", schema:" + mv.getOriginalSchema() + ", name:" + mv.getName() + ", " +
                "definition: " + mv.getDefStr());
        MVContext context = initializeMVContext(mv);
        if(context == null) {
            return;
        }

        // initialized transaction.
        MVInitliazeGlobalTransaction globalTran = new MVInitliazeGlobalTransaction(mv.getId());
        GenerateMVExpressionTransaction genExprTran = new GenerateMVExpressionTransaction(this.dbCon, context);
        globalTran.join(genExprTran);
        SetupDeltaTableTransaction setupDeltaTran = new SetupDeltaTableTransaction(this.dbCon, mv.getId());
        globalTran.join(setupDeltaTran);
        TableMarkerTransaction markerTran = new TableMarkerTransaction(this.dbCon, mv.getId());
        globalTran.join(markerTran);
        // initialized done.

        globalTran.begin();
        try{
            genExprTran.generateMVExpression();
            for(BaseDeltaTable delta : context.getDeltaTableList()) {
                setupDeltaTran.setupDeltaTable(delta);
                markerTran.issueTableMarker(delta);
            }
            updateMVState(mv, MaterializedView.State.SETUP_FINISH);
            globalTran.commit();
        } catch (Exception ex) {
            globalTran.rollback();
            try {
                updateMVState(mv, MaterializedView.State.SETUP_ERROR);
            } catch (SQLException ignore) {
            }
        }
    }

    private MVContext initializeMVContext(MaterializedView mv) {
        List<SQLStatement> stmtList = SQLUtils.parseStatements(mv.getDefStr(), JdbcConstants.MYSQL);
        MySqlSelectQueryBlock selectStmt = (MySqlSelectQueryBlock) (((SQLSelectStatement) stmtList.get(0)).getSelect()).getQuery();
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
        visitor.visit(selectStmt);
        return context;
    }

    public void updateMVState(MaterializedView mv, MaterializedView.State state) throws SQLException {
        String sql = "update mview set mview_status = ? where mview_id = ?";
        PreparedStatement pstmt = null;
        try{
            pstmt = dbCon.prepareStatement(sql);
            pstmt.setInt(1, state.getStateValue());
            pstmt.setLong(2, mv.getId());
            pstmt.executeUpdate();
        }  finally {
            try {
                pstmt.close();
            } catch (Exception ignore) {
            }
        }
    }
}
