package org.mysqlmv.mvm.sql;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.druid.sql.ast.statement.SQLUnique;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.util.JdbcConstants;
import org.mysqlmv.mvm.mv.SqlRewriteVisitor;

import java.util.List;

/**
 * Created by Kelvin Li on 6/2/2015.
 */
public class CreateTableSqlModifier implements SqlModifier {

    private MySqlCreateTableStatement createStmt;

    private static List<SQLTableElement> date_qty_columns;

    static {
        String masterSql = "CREATE TABLE `date_quatity_table` (`mysqlmv_dt` long, `mysqlmv_qty` int)";
        List<SQLStatement> stmtList1 = SQLUtils.parseStatements(masterSql, JdbcConstants.MYSQL);
        date_qty_columns = ((MySqlCreateTableStatement)stmtList1.get(0)).getTableElementList();
    }

    @Override
    public void modify(String sql) {
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        createStmt = (MySqlCreateTableStatement)stmtList.get(0);
        remoteUniqueConstraint();
        addDateQtyColumn();
    }

    private void addDateQtyColumn() {
        createStmt.getTableElementList().add(0, date_qty_columns.get(0));
        createStmt.getTableElementList().add(1, date_qty_columns.get(1));
    }

    private void remoteUniqueConstraint() {
        List<SQLTableElement> tableElements = createStmt.getTableElementList();
        for(int i =0; i<tableElements.size(); ) {
            if(tableElements.get(i) instanceof SQLUnique) {
                tableElements.remove(i);
            } else {
                i ++;
            }
        }
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
