package org.mysqlmv.mvm.sql.transaction;

import org.mysqlmv.mvm.mv.BaseDeltaTable;
import org.mysqlmv.mvm.mv.TableMarkerMvMapping;
import org.mysqlmv.mvm.sql.transaction.exception.BeginTransactionException;
import org.mysqlmv.mvm.sql.transaction.exception.CommitTransactionException;
import org.mysqlmv.mvm.sql.transaction.exception.RollbackTransactionException;
import org.mysqlmv.mvm.sql.transaction.exception.TransactionException;
import org.slf4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Kelvin Li on 6/3/2015.
 */
public class TableMarkerTransaction implements ITransaction {

    public static Logger logger = org.slf4j.LoggerFactory.getLogger(TableMarkerTransaction.class);

    private final long materializedViewId;

    private final Connection dbCon;

    private List<BaseDeltaTable> deltaTableList = new ArrayList<BaseDeltaTable>();

    private List<TableMarkerMvMapping> mappingList = new ArrayList<TableMarkerMvMapping>();

    public TableMarkerTransaction(Connection dbCon, long mvId) {
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
        mappingList.removeAll(mappingList);
    }

    @Override
    public void rollback() throws RollbackTransactionException {
        for (TableMarkerMvMapping mapping : mappingList) {
            remoteTableMarkerMvMapping(mapping);
        }
        mappingList.removeAll(mappingList);
        for (BaseDeltaTable delta : deltaTableList) {
            removeTableMarker(delta);
        }
        deltaTableList.removeAll(deltaTableList);
    }

    public void issueTableMarker(BaseDeltaTable delta) {
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            pstmt = dbCon.prepareStatement("select id from mview_table_marker where table_schema = ?  and table_name= ?");
            pstmt.setString(1, delta.getSchema());
            pstmt.setString(2, delta.getTable());
            rs = pstmt.executeQuery();
            if (rs.next()) {
                delta.setMarkerId(rs.getLong(1));
            } else {
                rs.close();
                pstmt.close();
                String markerSql = "insert into mview_table_marker(table_schema, table_name) values(?, ?)";
                pstmt = dbCon.prepareStatement(markerSql, Statement.RETURN_GENERATED_KEYS);
                pstmt.setString(1, delta.getSchema());
                pstmt.setString(2, delta.getTable());
                pstmt.executeUpdate();
                rs = pstmt.getGeneratedKeys();
                rs.next();
                long markerId = rs.getLong(1);
                delta.setMarkerId(markerId);
                deltaTableList.add(delta);
            }
            TableMarkerMvMapping mapping = new TableMarkerMvMapping(delta.getMarkerId(), materializedViewId);
            createTableMarkerMvMapping(mapping);
        } catch (SQLException e) {
            logger.error("Error when mark table, schema:" + delta.getSchema() + ", table:" + delta.getTable(), e);
            throw new TransactionException(getTransactionID(), e);
        } finally {
            try {
                rs.close();
                pstmt.close();
                rs = null;
                pstmt = null;
            } catch (Exception ignore) {
            }
        }
    }

    private void createTableMarkerMvMapping(TableMarkerMvMapping mapping) {
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            String markerMviewSql = "insert into mview_delta_mapping(mview_id, table_marker_id) values(? ,?)";
            pstmt = dbCon.prepareStatement(markerMviewSql);
            pstmt.setLong(1, mapping.getMvId());
            pstmt.setLong(2, mapping.getTableMarkerId());
            pstmt.executeUpdate();
            mappingList.add(mapping);
        } catch (SQLException e) {
            logger.error("Error when creating table marker and mv mapping, mv id:" + mapping.getMvId() +
                    ", marker id:" + mapping.getTableMarkerId(), e);
            throw new TransactionException(getTransactionID(), e);
        } finally {
            try {
                rs.close();
                pstmt.close();
                rs = null;
                pstmt = null;
            } catch (Exception ignore) {
            }
        }
    }

    private void removeTableMarker(BaseDeltaTable delta) {
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            String removeMarkerSql = "delete from mview_table_maker where table_schema = ? and table_name = ?";
            pstmt = dbCon.prepareStatement(removeMarkerSql);
            pstmt.setString(1, delta.getSchema());
            pstmt.setString(2, delta.getTable());
            pstmt.executeUpdate();
            pstmt.close();
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

    private void remoteTableMarkerMvMapping(TableMarkerMvMapping mapping) {
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            String remoteMarkerMviewSql = "delete from mview_delta_mapping where mview_id = ? and table_marker_id = ?";
            pstmt = dbCon.prepareStatement(remoteMarkerMviewSql);
            pstmt.setLong(1, mapping.getMvId());
            pstmt.setLong(2, mapping.getTableMarkerId());
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
