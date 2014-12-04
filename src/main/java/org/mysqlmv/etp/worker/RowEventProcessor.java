package org.mysqlmv.etp.worker;

import org.mysqlmv.cd.logevent.Event;
import org.mysqlmv.cd.logevent.eventdef.data.*;
import org.mysqlmv.cd.logevent.parser.impl.TableMapContext;
import org.mysqlmv.common.io.db.DBUtil;
import org.mysqlmv.common.io.db.QueryCallBack;
import org.mysqlmv.etp.context.EoiContext;
import org.mysqlmv.etp.context.ToiContext;
import org.mysqlmv.etp.context.ToiEntry;
import org.mysqlmv.etp.context.ToiValue;
import org.mysqlmv.etp.scanner.MysqlMVConstant;
import org.slf4j.Logger;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

/**
 * Created by Kelvin Li on 12/1/2014 5:17 PM.
 */
public class RowEventProcessor implements Runnable {
    public static Logger logger = org.slf4j.LoggerFactory.getLogger(RowEventProcessor.class);

    private Event rowEvent;

    private String schema;

    private String table;

    @Override
    public void run() {
        try {
            logger.info("event will be processed");
            runTask();
            logger.info("event processed");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void runTask() throws SQLException {
        RowsEventData data = rowEvent.getData();
        TableMapEventData tMap = TableMapContext.getTableMap(data.getTableId());
        schema = tMap.getDbName();
        table = tMap.getTableName();
        int ordinal = this.findPKOrdinal(schema, table);
        List<RowsEventData.Row> rows = data.getRows();
        if(data instanceof DeleteRowsEventData) {
            processDelete(ordinal);
        } else if(data instanceof WriteRowsEventData) {
            processInsert(ordinal);
        } else if(data instanceof UpdateRowsEventData) {
            processUpdate(ordinal);
        }
    }

    private void processInsert(int idOrdinal) throws SQLException {
        RowsEventData data = rowEvent.getData();
        Set<ToiValue> vValueList = ToiContext.getToiValue(new ToiEntry(schema, table));
        for(RowsEventData.Row row: data.getRows()) {
            for(ToiValue toiValue : vValueList) {
                int id = (Integer)row.getCells().get(idOrdinal - 1).getValue();
                int toiId = toiValue.getMviewToiId();
                insertTOI(id, toiId, RowOperation.INSERT);
            }

        }
    }

    private void processDelete(int idOrdinal) throws SQLException {
        RowsEventData data = rowEvent.getData();
        Set<ToiValue> vValueList = ToiContext.getToiValue(new ToiEntry(schema, table));
        for(RowsEventData.Row row: data.getRows()) {
            for(ToiValue toiValue : vValueList) {
                int id = (Integer)row.getCells().get(idOrdinal - 1).getValue();
                int toiId = toiValue.getMviewToiId();
                insertTOI(id, toiId, RowOperation.DELETE);
            }

        }
    }

    private void processUpdate(int idOrdinal) throws SQLException {
        RowsEventData data = rowEvent.getData();
        Set<ToiValue> tValueList = ToiContext.getToiValue(new ToiEntry(schema, table));
        for(int i=0; i<data.getRows().size();) {
            int id = (Integer) data.getRows().get(i).getCells().get(idOrdinal - 1).getValue();
            for (ToiValue toiValue : tValueList) {
                int toiId = toiValue.getMviewToiId();
                insertTOI(id, toiId, RowOperation.UPDATE_D);
            }
            i++;
            for (ToiValue toiValue : tValueList) {
                int toiId = toiValue.getMviewToiId();
                insertTOI(id, toiId, RowOperation.UPDATE_I);
            }
            i++;
        }
    }



    private void insertTOI(final int rec_id,
                           final int mview_toi_id, final RowOperation opr_type) throws SQLException {
        DBUtil.executeInPrepareStmt(new QueryCallBack() {
            @Override
            public String getSql() {
                return "insert into " + String.format(MysqlMVConstant.TABLE_NAME_FORMAT, schema, table)
                        +"(rec_id, mview_toi_id, opr_type, is_applied, create_datetime) " +
                        "values(?, ?, ?, 0, now())";
            }
            @Override
            public Object doInCallback(PreparedStatement pstmt) throws SQLException {
                pstmt.setInt(1, rec_id);
                pstmt.setInt(2, mview_toi_id);
                pstmt.setInt(3, opr_type.getValue());
                pstmt.execute();
                return null;
            }
        });
    }

    private int findPKOrdinal(final String schema, final String table) throws SQLException {
        return (Integer) DBUtil.executeInPrepareStmt(new QueryCallBack<Integer>() {
            @Override
            public String getSql() {
                return "select ordinal_position from information_schema.columns " +
                        "where table_schema = ? and table_name = ? " +
                        "and column_key = 'PRI';";
            }
            @Override
            public Integer doInCallback(PreparedStatement pstmt) throws SQLException {
                pstmt.setString(1, schema);
                pstmt.setString(2, table);
                pstmt.execute();
                rs = pstmt.getResultSet();
                if(rs.next()) {
                    return rs.getInt(1);
                }
                return null;
            }
        });
    }

    public RowEventProcessor(Event rowEvent) {
        this.rowEvent = rowEvent;
    }
}
