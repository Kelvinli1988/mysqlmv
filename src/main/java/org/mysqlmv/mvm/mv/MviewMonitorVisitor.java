package org.mysqlmv.mvm.mv;

import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import org.slf4j.Logger;

/**
 * Created by Kelvin Li on 11/24/2014 1:16 PM.
 */
public class MviewMonitorVisitor extends MySqlOutputVisitor {

    public static Logger logger = org.slf4j.LoggerFactory.getLogger(MviewMonitorVisitor.class);

    private MVContext context;

    static StringBuilder sb = new StringBuilder();

    public MviewMonitorVisitor() {
        super(sb);
    }

    public MviewMonitorVisitor(Appendable appender) {
        super(appender);
    }

    public MVContext getContext() {
        return context;
    }

    public void setContext(MVContext context) {
        this.context = context;
    }

    public boolean visit(com.alibaba.druid.sql.ast.statement.SQLExprTableSource param0) {
        logger.debug(param0.getClass().getName());
        handleSimpleSource(param0);
        return super.visit(param0);
    }

    private void handleSimpleSource(SQLExprTableSource from) {
        String[] exprs = from.getExpr().toString().split("\\.");
        String schema = exprs[0].replace("`", "");
        String table = exprs[1].replace("`", "");
        String alias = from.getAlias() == null ? "" : from.getAlias();
        context.getDeltaTableList().add(new BaseDeltaTable(context.getMview().getId(), schema, table, alias));
    }
}
