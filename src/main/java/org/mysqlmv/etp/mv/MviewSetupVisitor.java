package org.mysqlmv.etp.mv;

import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import org.mysqlmv.etp.dao.EtpDao;
import org.mysqlmv.mvm.mv.MviewMonitorVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Kelvin Li on 11/24/2014 3:34 PM.
 */
public class MviewSetupVisitor extends MviewMonitorVisitor {

    Logger logger = LoggerFactory.getLogger(MviewMonitorVisitor.class);

    public boolean visit(com.alibaba.druid.sql.ast.statement.SQLExprTableSource param0) {
        logger.debug(param0.getClass().getName());
        handleSimpleSource(param0);
        return super.visit(param0);
    }

    private void handleSimpleSource(SQLExprTableSource from) {
        // insert table of interest
        String[] exprs = from.getExpr().toString().split("\\.");
        String schema = exprs[0].replace("`", "");
        String table = exprs[1].replace("`", "");
        String alias = from.getAlias() == null ? "" : from.getAlias();
        EtpDao.addMviewToiDef(getContext().getMview().getId(), schema, table, alias);
    }
}