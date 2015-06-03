package org.mysqlmv.mvm.sql.transaction;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.util.JdbcConstants;
import org.mysqlmv.mvm.mv.MVContext;
import org.mysqlmv.mvm.mv.SqlRewriteVisitor;
import org.mysqlmv.mvm.sql.transaction.exception.BeginTransactionException;
import org.mysqlmv.mvm.sql.transaction.exception.CommitTransactionException;
import org.mysqlmv.mvm.sql.transaction.exception.RollbackTransactionException;
import org.mysqlmv.mvm.sql.transaction.exception.TransactionException;
import org.slf4j.Logger;

import java.sql.*;
import java.util.List;

/**
 * Created by Kelvin Li on 6/3/2015.
 */
public class GenerateMVExpressionTransaction implements ITransaction {

    public static Logger logger = org.slf4j.LoggerFactory.getLogger(GenerateMVExpressionTransaction.class);

    private final MVContext context;

    private final Connection dbCon;

    MySqlSelectQueryBlock selectStmt;

    public GenerateMVExpressionTransaction(Connection conn, MVContext context) {
        this.dbCon = conn;
        this.context = context;
    }

    @Override
    public long getTransactionID() {
        return context.getMview().getId();
    }

    @Override
    public void begin() throws BeginTransactionException {
        List<SQLStatement> stmtList = SQLUtils.parseStatements(context.getMview().getDefStr(), JdbcConstants.MYSQL);
        selectStmt = (MySqlSelectQueryBlock) (((SQLSelectStatement) stmtList.get(0)).getSelect()).getQuery();
    }

    @Override
    public void commit() throws CommitTransactionException {
        // DO NOTHING
    }

    @Override
    public void rollback() throws RollbackTransactionException {
        String sql = "delete from mview_expression where mview_id = ?";
        PreparedStatement pstmt = null;
        try {
            pstmt = dbCon.prepareStatement(sql);
            pstmt.setLong(1, context.getMview().getId());
            pstmt.executeUpdate();
        } catch (SQLException e) {
            logger.error("Error when rollback transaction.", e);
        } finally {
            try {
                pstmt.close();
            } catch (Exception ignore) {
            }
        }
    }

    public void generateMVExpression() throws TransactionException {
        generateSelectExpression(selectStmt, context);
        generateFromExpression(selectStmt.getFrom(), context);
        generateWhereExpression(selectStmt, context);
    }

    private long generateFromExpression(Object from, MVContext context) throws TransactionException {
        String sql = "insert into mview_expression(mview_id, type, table_owner, table_name, table_alias) values(?, 'from', ?, ?, ?)";
        if(from instanceof SQLJoinTableSource) {
            return generateJoinExpression((SQLJoinTableSource)from, context);
        } else if(from instanceof SQLExprTableSource) {
            SQLExprTableSource table = (SQLExprTableSource)from;
            SQLPropertyExpr prop = (SQLPropertyExpr)table.getExpr();
            String name = prop.getName();
            SQLIdentifierExpr ownerExpr = (SQLIdentifierExpr)prop.getOwner();
            String owner = ownerExpr.getName();
            PreparedStatement pstmt = null;
            ResultSet rs = null;
            try{
                pstmt = dbCon.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                pstmt.setLong(1, context.getMview().getId());
                pstmt.setString(2, owner);
                pstmt.setString(3, name);
                pstmt.setString(4, table.getAlias());
                pstmt.executeUpdate();
                rs = pstmt.getGeneratedKeys();
                rs.next();
                return rs.getLong(1);
            } catch (SQLException e) {
                logger.error("Error when generate from expression.", e);
                throw new TransactionException(getTransactionID(), e);
            } finally {
                try {
                    rs.close();
                    pstmt.close();
                } catch (Exception ignore) {
                }
            }
        } else {
            throw new TransactionException(getTransactionID(), "Generate from clause should not arrive here");
        }
    }

    private long generateJoinExpression(SQLJoinTableSource join, MVContext context) throws TransactionException {
        long leftId = generateFromExpression(join.getLeft(), context);
        long rightId = generateFromExpression(join.getRight(), context);
        SQLBinaryOpExpr conditionExpr = (SQLBinaryOpExpr)join.getCondition();
        SqlRewriteVisitor visitor = new SqlRewriteVisitor();
        visitor.visit(conditionExpr);
        String condition = visitor.toString();
        long thisId = 0L;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = "insert into mview_expression(mview_id, type, join_type, join_left, " +
                "join_right, expression) values(?, 'join', ?, ?, ?, ?)";
        try {
            pstmt = dbCon.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            pstmt.setLong(1, context.getMview().getId());
            pstmt.setString(2, join.getJoinType().toString());
            pstmt.setLong(3, leftId);
            pstmt.setLong(4, rightId);
            pstmt.setString(5, condition);
            pstmt.executeUpdate();
            rs = pstmt.getGeneratedKeys();
            rs.next();
            thisId = rs.getLong(1);
        } catch (SQLException e) {
            logger.error("Error when generating join expression", e);
            throw new TransactionException(getTransactionID(), e);
        } finally {
            try {
                rs.close();
                pstmt.close();
            } catch (Exception ignore) {
            }
        }
        return thisId;
    }

    private void generateSelectExpression(MySqlSelectQueryBlock stmt, MVContext context) {
        List<SQLSelectItem> selectList = stmt.getSelectList();
        if(selectList != null && selectList.size() > 0) {
            String sql = "insert into mview_expression(type, mview_id, expression, expr_order) values('select', ?, ?, ?)";
            PreparedStatement pstmt = null;
            try {
                pstmt = dbCon.prepareStatement(sql);
                for(int seq=0; seq < selectList.size(); seq ++) {
                    pstmt.setLong(1, context.getMview().getId());
                    pstmt.setString(2, selectList.get(seq).toString());
                    pstmt.setInt(3, seq);
                    pstmt.executeUpdate();
                }
            } catch (SQLException e) {
                logger.error("Error when generating select expression", e);
                throw new TransactionException(getTransactionID(), e);
            } finally {
                try {
                    pstmt.close();
                } catch (Exception ignore) {
                }
            }
        }
    }

    /**
     * TODO:
     * Actually where is not so import at current stage.
     * The condition expression must always begin with a alias.
     * But need to add validation to ensure the alias exists.
     */
    private void generateWhereExpression(MySqlSelectQueryBlock stmt, MVContext context) {
        SQLExpr whereExpr = stmt.getWhere();
        if(whereExpr == null) {
            return;
        }
        SQLBinaryOpExpr where = (SQLBinaryOpExpr)whereExpr;
        SqlRewriteVisitor visitor = new SqlRewriteVisitor();
        visitor.visit(where);
        String whereStr = visitor.toString();
        String sql = "insert into mview_expression(type, mview_id, expression) values('where', ?, ?)";
        PreparedStatement pstmt = null;
        try {
            pstmt = dbCon.prepareStatement(sql);
            pstmt.setLong(1, context.getMview().getId());
            pstmt.setString(2, whereStr);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            logger.error("Error when generating where expression", e);
            throw new TransactionException(getTransactionID(), e);
        } finally {
            try {
                pstmt.close();
            } catch (Exception ignore) {
            }
        }
    }

}
