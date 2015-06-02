package org.mysqlmv.mvm.sql;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.util.JdbcConstants;
import org.mysqlmv.mvm.mv.SqlRewriteVisitor;

import java.util.List;

/**
 * Created by I312762 on 6/2/2015.
 */
public class ChangeTableOwnerModifier implements SqlModifier{

    public static final String DEFAULT_OWNER = "mysqlmv";

    private MySqlCreateTableStatement createStmt;

    private String newOwner;

    private SqlModifier previsouModifier;

    public ChangeTableOwnerModifier() {
        newOwner = DEFAULT_OWNER;
    }

    public ChangeTableOwnerModifier(String newOwner) {
        this.newOwner = newOwner;
    }

    public ChangeTableOwnerModifier(SqlModifier previsouModifier) {
        this.previsouModifier = previsouModifier;
    }

    @Override
    public ChangeTableOwnerModifier modify(String sql) {
        if(previsouModifier != null) {
            sql = previsouModifier.modify(sql).getResult();
        }
        List<SQLStatement> stmtList1 = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        createStmt = (MySqlCreateTableStatement)stmtList1.get(0);
        SQLExprTableSource tableSource = createStmt.getTableSource();
        ((SQLIdentifierExpr)((SQLPropertyExpr)tableSource.getExpr()).getOwner()).setName(newOwner);
        return this;
    }

    public String getOwner() {
        return newOwner;
    }

    @Override
    public String toString() {
        SqlRewriteVisitor visitor = new SqlRewriteVisitor();
        visitor.visit(createStmt);
        return visitor.toString();
    }

    @Override
    public String getResult() {
        return toString();
    }

}
