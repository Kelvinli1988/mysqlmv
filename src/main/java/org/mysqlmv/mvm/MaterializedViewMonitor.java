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
import org.mysqlmv.mvm.sql.CreateTableSqlModifier;
import org.slf4j.Logger;

import java.sql.*;
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
        Statement stmt = null;
        ResultSet rs = null;
        try {
            dbCon.createStatement();
            stmt = dbCon.createStatement();
            String deltaCountSql = "select count(1)  from information_schema.tables where table_schema='mysqlmv' and table_name = '" + delta.getTable() + "'";
            stmt.execute(deltaCountSql);
            rs = stmt.getResultSet();
            rs.next();
            int deltaCount = rs.getInt(1);
            rs.close();
            if (deltaCount != 0) {
                return;
            }
            String sql = "show create table " + delta.getSchema() + "." + delta.getTable();
            stmt.execute(sql);
            rs = stmt.getResultSet();
            String tblDef = null;
            while (rs.next()) {
                tblDef = rs.getString("Create Table");
            }
            rs.close();
            if (tblDef != null) {
                CreateTableSqlModifier createModifier = new CreateTableSqlModifier();
                String newTable = createModifier.modify(tblDef).getResult();
                stmt.execute(newTable);
                issueTableMarker(delta);
            }
        } catch (SQLException e) {
            logger.error("Error while finding uninitalized materialized view.", e);
        } finally {
            try {
                stmt.close();
                rs.close();
            } catch (Exception ignore) {
            }
        }
    }

    private void issueTableMarker(BaseDeltaTable delta) {
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            String markerSql = "insert into mview_table_marker(table_schema, table_name) values(?, ?)";
            pstmt = dbCon.prepareStatement(markerSql, Statement.RETURN_GENERATED_KEYS);
            pstmt.setString(1, delta.getSchema());
            pstmt.setString(2, delta.getTable());
            pstmt.executeUpdate();
            rs = pstmt.getGeneratedKeys();
            rs.next();
            long markerId = rs.getLong(1);
            rs.close();
            pstmt.close();
            String markerMviewSql = "insert into mview_delta_mapping(mview_id, table_marker_id) values(? ,?)";
            pstmt = dbCon.prepareStatement(markerMviewSql);
            pstmt.setLong(1, delta.getMvID());
            pstmt.setLong(2, markerId);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            logger.error("Error while finding uninitalized materialized view.", e);
        } finally {
            try {
                rs.close();
                pstmt.close();
            } catch (Exception ignore) {
            }
        }
    }
}
