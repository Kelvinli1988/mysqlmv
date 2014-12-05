package org.mysqlmv.etp.mv;

import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import org.mysqlmv.common.io.db.ConnectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by Kelvin Li on 11/24/2014 3:34 PM.
 */
public class MviewSetupVisitor extends MviewMonitorVisitor {

    Logger logger = LoggerFactory.getLogger(MviewMonitorVisitor.class);

    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLExprTableSource param0) {
        logger.debug(param0.getClass().getName());
        handleSimpleSource(param0);
        return super.visit(param0);
    }

    private void handleSimpleSource(SQLExprTableSource from) {
        // insert table of interest
        String insertSqlStr = "insert into mview_toi(mview_toi_id, mview_id, schema_name, table_name, alias, create_datetime, last_update_datetime) " +
                "values(null, ?, ?, ?, ?, now(), now())";
        try {

            String[] exprs = from.getExpr().toString().split("\\.");
            PreparedStatement pstmt = ConnectionUtil.getConnection().prepareStatement(insertSqlStr);
            pstmt.setInt(1, getContext().getMview().getId());
            String schema = exprs[0].replace("`", "");
            String table = exprs[1].replace("`", "");
            pstmt.setString(2, schema);
            pstmt.setString(3, table);
            String alias = from.getAlias() == null ? "" : from.getAlias();
            pstmt.setString(4, alias.replace("`", ""));
            logger.info("Insert mview_toi record, schema:" + schema + ", table:" + table);
            pstmt.execute();
            pstmt.close();
        } catch (SQLException e) {
            logger.error("Fail to insert mview_toi record.", e);
            throw new RuntimeException(e);
        }
    }
}