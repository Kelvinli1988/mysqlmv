package org.mysqlmv.mvm.sql.transaction;

import org.mysqlmv.mvm.mv.BaseDeltaTable;
import org.mysqlmv.mvm.sql.CreateTableSqlModifier;
import org.mysqlmv.mvm.sql.transaction.exception.BeginTransactionException;
import org.mysqlmv.mvm.sql.transaction.exception.CommitTransactionException;
import org.mysqlmv.mvm.sql.transaction.exception.RollbackTransactionException;
import org.mysqlmv.mvm.sql.transaction.exception.TransactionException;
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Kelvin Li on 6/3/2015.
 */
public class SetupDeltaTableTransaction implements ITransaction {

    public static Logger logger = org.slf4j.LoggerFactory.getLogger(SetupDeltaTableTransaction.class);

    private final long materializedViewId;

    private final Connection dbCon;

    private List<BaseDeltaTable> deltaTableList = new ArrayList<BaseDeltaTable>();

    public SetupDeltaTableTransaction(Connection dbCon, long mvId) {
        this.dbCon = dbCon;
        this.materializedViewId = mvId;
    }

    @Override
    public long getTransactionID() {
        return materializedViewId;
    }

    @Override
    public void begin() throws BeginTransactionException {
        // DO NOTHING
    }

    @Override
    public void commit() throws CommitTransactionException {
        deltaTableList.removeAll(deltaTableList);
    }

    @Override
    public void rollback() throws RollbackTransactionException {
        for(BaseDeltaTable delta : deltaTableList) {
            dropDeltaTable(delta);
        }
        deltaTableList.removeAll(deltaTableList);
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
                deltaTableList.add(delta);
            }
        } catch (SQLException e) {
            logger.error("Error when setup delta table, schema : " + delta.getSchema()
                    + ", table:" + delta.getTable(), e);
            throw new TransactionException(getTransactionID(), e);
        } finally {
            try {
                stmt.close();
                rs.close();
            } catch (Exception ignore) {
            }
        }
    }

    private void dropDeltaTable(BaseDeltaTable delta) {
        Statement stmt = null;
        ResultSet rs = null;
        try {
            dbCon.createStatement();
            stmt = dbCon.createStatement();
            stmt.execute("drop table " + delta.getTable());
        } catch (SQLException e) {
            logger.error("Error when dropping delta table, table name : " + delta.getTable(), e);
        } finally {
            try {
                stmt.close();
                rs.close();
            } catch (Exception ignore) {
            }
        }
    }
}
