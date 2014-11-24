package org.mysqlmv.etp.mv;

import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateUserStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import org.mysqlmv.common.io.db.ConnectionUtil;
import org.mysqlmv.etp.scanner.MVContext;
import org.slf4j.Logger;

import java.sql.PreparedStatement;
import java.sql.SQLException;

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

    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLExprTableSource param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }

    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowBinaryLogsStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowMasterLogsStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCollationStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.MySqlIgnoreIndexHint param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUnlockTablesStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSetCharSetStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlLockTableStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowAuthorsStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateDatabaseStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUnionQuery param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateEventStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateFunctionStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowBinLogEventsStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCharacterSetStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.MySqlUseIndexHint param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowContributorsStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlPartitionByKey param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableCharacter param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlPartitionByRange param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlPartitionByList param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableModifyColumn param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlResetStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateUserStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableOption param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (MySqlCreateUserStatement.UserSpecification param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.MySqlForceIndexHint param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSetTransactionIsolationLevelStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSetNamesStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlPartitionByHash param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlOutFileExpr param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableChangeColumn param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDescribeStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowKeysStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowMasterStatusStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowSlaveStatusStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowOpenTablesStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowPluginsStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowTriggersStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlUserName param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowIndexesStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowTableStatusStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowProcedureStatusStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowProcessListStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowProfilesStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowProfileStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowSlaveHostsStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowPrivilegesStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowProcedureCodeStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowRelayLogEventsStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateIndexStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateViewStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowEngineStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableAddColumn param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlRenameTableStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateProcedureStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateTableStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlRenameTableStatement.Item param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowEnginesStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowVariantsStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowErrorsStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowGrantsStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowEventsStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowFunctionCodeStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowFunctionStatusStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSetPasswordStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlLoadDataInFileStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlReplaceStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlPartitioningDef param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlPartitioningDef.LessThanValues param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLAssignItem param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlIntervalExpr param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlStartTransactionStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCommitStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectGroupBy param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement.TableSpaceOption param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlMatchAgainstExpr param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAnalyzeStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlBinaryExpr param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlOptimizeStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlExtractExpr param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlPartitioningDef.InValues param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlExecuteStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.MySqlKey param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterUserStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.MySqlPrimaryKey param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlPrepareStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.expr.SQLCharExpr param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowStatusStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlLoadXmlStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlCharExpr param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLColumnDefinition param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.MySqlUnique param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowWarningsStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlKillStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlHelpStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlBinlogStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateTriggerStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.CobarShowStatus param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowColumnsStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowTablesStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.SQLDataType param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableImportTablespace param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLCharacterDataType param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlHintStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlRollbackStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.MysqlForeignKey param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableDiscardTablespace param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock.Limit param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowDatabasesStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowTriggersStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowSlaveHostsStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlOptimizeStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSetPasswordStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowProfileStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterUserStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowTableStatusStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowProfilesStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowRelayLogEventsStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowSlaveStatusStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlLockTableStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableDiscardTablespace param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUnlockTablesStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.MySqlUnique param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableImportTablespace param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.MySqlIgnoreIndexHint param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.MySqlUseIndexHint param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableModifyColumn param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlHelpStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableCharacter param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableOption param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlCharExpr param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableChangeColumn param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.MySqlForceIndexHint param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableAddColumn param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlPartitioningDef.InValues param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateIndexStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlRenameTableStatement.Item param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAnalyzeStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowVariantsStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement.TableSpaceOption param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUnionQuery param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlPartitioningDef param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlRenameTableStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlPartitioningDef.LessThanValues param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowStatusStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlLoadXmlStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.CobarShowStatus param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlKillStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlBinlogStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlRollbackStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowColumnsStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowTablesStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowDatabasesStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowWarningsStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlPartitionByRange param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlPartitionByList param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlPartitionByHash param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlOutFileExpr param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlResetStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateUserStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateUserStatement.UserSpecification param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlPartitionByKey param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.MysqlForeignKey param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlIntervalExpr param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlExtractExpr param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlMatchAgainstExpr param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlBinaryExpr param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock.Limit param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.MySqlKey param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.MySqlPrimaryKey param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlLoadDataInFileStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlReplaceStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectGroupBy param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlStartTransactionStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCommitStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlPrepareStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlExecuteStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowFunctionStatusStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowEnginesStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowErrorsStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowGrantsStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlUserName param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateTriggerStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateViewStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowEngineStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowEventsStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowFunctionCodeStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowPluginsStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowPrivilegesStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowProcedureCodeStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowProcedureStatusStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowProcessListStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowIndexesStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowKeysStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowMasterStatusStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowOpenTablesStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowBinaryLogsStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowMasterLogsStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCollationStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowBinLogEventsStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSetTransactionIsolationLevelStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSetNamesStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSetCharSetStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowAuthorsStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateEventStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateFunctionStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateProcedureStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateTableStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlHintStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCharacterSetStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowContributorsStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateDatabaseStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDescribeStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }

    public void printValuesList (com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement param0) {
        logger.debug(param0.getClass().getName());
        super.printValuesList(param0);
    }

    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLAlterTableAddColumn param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLAlterTableDropColumnItem param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLDropIndexStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLSavePointStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLReleaseSavePointStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLTruncateStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.expr.SQLDefaultExpr param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLCommentStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLUseStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLAlterTableDropIndex param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.SQLOver param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLColumnPrimaryKey param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLColumnUniqueKey param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLRollbackStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.SQLCommentHint param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLCreateDatabaseStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLCreateViewStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.expr.SQLUnaryExpr param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.expr.SQLHexExpr param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLSetStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }

    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLCallStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLUniqueConstraint param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.NotNullConstraint param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLUnionQueryTableSource param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.expr.SQLAllExpr param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.expr.SQLInSubQueryExpr param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.expr.SQLListExpr param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLSubqueryTableSource param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLJoinTableSource param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLInsertStatement.ValuesClause param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.expr.SQLSomeExpr param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.expr.SQLAnyExpr param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLDropSequenceStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLDropTriggerStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLDropUserStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLExplainStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLGrantStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLPrimaryKeyImpl param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLAlterTableRenameColumn param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLColumnReference param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLForeignKeyImpl param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLAlterTableAddIndex param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLAlterTableAddConstraint param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLCreateTriggerStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.expr.SQLBooleanExpr param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLDropDatabaseStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLDropFunctionStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLDropTableSpaceStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLDropProcedureStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLCheck param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLAlterTableDropForeignKey param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLAlterTableDropPrimaryKey param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLAlterTableEnableKeys param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLColumnCheck param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLWithSubqueryClause param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }

    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLAlterTableAlterColumn param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLExprHint param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLCreateIndexStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLUnique param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLAlterTableDisableKeys param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLAlterTableDisableConstraint param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLAlterTableEnableConstraint param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLAlterTableDropConstraint param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.expr.SQLNCharExpr param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.expr.SQLAggregateExpr param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }

    public boolean visit (com.alibaba.druid.sql.ast.expr.SQLIntegerExpr param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.expr.SQLInListExpr param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.expr.SQLQueryExpr param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.expr.SQLPropertyExpr param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.expr.SQLNumberExpr param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.expr.SQLNullExpr param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.expr.SQLNotExpr param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.expr.SQLCastExpr param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }

    public boolean visit (com.alibaba.druid.sql.ast.expr.SQLCaseExpr param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.expr.SQLBetweenExpr param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLUnionQuery param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.expr.SQLExistsExpr param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLTableElement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLDropViewStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLDropTableStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }

    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLSelectStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLUpdateStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLUpdateSetItem param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLInsertStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.expr.SQLCurrentOfCursorExpr param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLDeleteStatement param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.SQLOrderBy param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLSelectItem param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }

    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLSelect param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLSelectGroupByClause param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }
    public boolean visit (com.alibaba.druid.sql.ast.statement.SQLSelectOrderByItem param0) {
        logger.debug(param0.getClass().getName());
        return super.visit(param0);
    }

    public void endVisit (com.alibaba.druid.sql.ast.statement.SQLDropSequenceStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.ast.statement.SQLDropUserStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.ast.statement.SQLAlterTableAddColumn param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.ast.expr.SQLBooleanExpr param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void endVisit (com.alibaba.druid.sql.ast.statement.SQLCommentStatement param0) {
        logger.debug(param0.getClass().getName());
        super.endVisit(param0);
    }
    public void visitAggreateRest (com.alibaba.druid.sql.ast.expr.SQLAggregateExpr param0) {
        logger.debug(param0.getClass().getName());
        super.visitAggreateRest(param0);
    }

    public void visitColumnDefault (com.alibaba.druid.sql.ast.statement.SQLColumnDefinition param0) {
        logger.debug(param0.getClass().getName());
        super.visitColumnDefault(param0);
    }
}
