package org.mysqlmv.etp.scanner;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.util.JdbcConstants;
import org.mysqlmv.common.util.CollectionUtils;
import org.mysqlmv.common.util.db.DBUtil;
import org.mysqlmv.common.util.db.QueryCallBack;
import org.mysqlmv.etp.context.ToiContext;
import org.mysqlmv.etp.context.ToiEntry;
import org.mysqlmv.etp.context.ToiValue;
import org.mysqlmv.etp.dao.EtpDao;
import org.mysqlmv.etp.mv.MaterializedView;
import org.mysqlmv.etp.mv.MviewSetupVisitor;
import org.slf4j.Logger;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import static org.mysqlmv.common.util.db.DBUtil.logger;

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
        logger.info("Update mview definition, mview_id:" + mv.getId());
        EtpDao.updateMVDef(mv);
        setupTOI();
    }



    private void setupTOI() {
        List<ToiEntry> entryList = EtpDao.findUnsetupToiEntry();
        if (!CollectionUtils.isEmpty(entryList)) {
            for (ToiEntry entry : entryList) {
                String schema = entry.getSchema();
                String table = entry.getTable();
                if (!EtpDao.isTableExisted(getTOITableName(entry.getSchema(), entry.getTable()))) {
                    logger.info("create cd_log table for schema:" + schema + ", table:" + table);
                    EtpDao.createToiTable(schema, table);
                }
                logger.info("Update toi setup as finished, schema:" + schema + ", table:" +table);
                EtpDao.updateToiSetup(schema, table);
                // Add toi setup into toi context
                logger.info("Add table into TOIContext, schema:" + schema + ", table:" +table);
                ToiContext.addToiEntry(new ToiEntry(schema, table), new ToiValue(DBUtil.getLastInsertedID(), mv.getId()));
            }
        }
    }

    private String getTOITableName(String schema, String table) {
        return String.format(MysqlMVConstant.TABLE_NAME_FORMAT, schema, table);
    }
}
