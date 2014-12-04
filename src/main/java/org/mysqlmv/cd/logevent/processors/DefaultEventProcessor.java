package org.mysqlmv.cd.logevent.processors;

import org.mysqlmv.cd.logevent.Event;
import org.mysqlmv.cd.logevent.EventProcessor;
import org.mysqlmv.cd.logevent.LogEventType;
import org.mysqlmv.cd.logevent.eventdef.data.RotateEventData;
import org.mysqlmv.cd.logevent.eventdef.data.RowsEventData;
import org.mysqlmv.cd.logevent.eventdef.data.TableMapEventData;
import org.mysqlmv.cd.logevent.parser.impl.TableMapContext;
import org.mysqlmv.common.io.db.DBUtil;
import org.mysqlmv.common.io.db.QueryCallBack;
import org.mysqlmv.etp.context.EoiContext;
import org.mysqlmv.etp.context.ToiContext;
import org.mysqlmv.etp.context.ToiEntry;
import org.mysqlmv.etp.worker.RowEventProcessService;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by Kelvin Li on 12/1/2014 2:06 PM.
 */
public class DefaultEventProcessor implements EventProcessor {
    @Override
    public void processEvent(Event event) {
        if(event.getHeader().getEventType().equals(LogEventType.ROTATE)) {
            try{
                processRotateEvent(event);
            } catch (SQLException ex) {

            }
        } else if(event.getData() instanceof RowsEventData) {
            if(EoiContext.isEoi(((RowsEventData) event.getData()).getTableId())) {
                processRowEvent(event);
            }
        } else if(event.getHeader().getEventType().equals(LogEventType.TABLE_MAP)) {
            processTableMapEvent(event);
        }
    }

    private void processRotateEvent(Event event) throws SQLException {
        RotateEventData rdata = event.getData();
        final String nextFile = rdata.getNameOfNextLog();
        final long newPos = rdata.getPosOfNextLog();
        final long eventTime = event.getHeader().getTimestamp();
        // 1. update rotate time.
        DBUtil.executeInPrepareStmt(new QueryCallBack() {
            @Override
            public String getSql() {
                return "update bin_log_file_logger set rotate_datatime = ? where logger_id in(select logger_id from bin_log_file_logger order by logger_id desc limit 1)";
            }

            @Override
            public Object doInCallback(PreparedStatement pstmt) throws SQLException {
                Date rotateData = new Date(eventTime);
                pstmt.setDate(1, rotateData);
                pstmt.executeUpdate();
                return null;
            }
        });
        // 2. insert new record log.
        DBUtil.executeInPrepareStmt(new QueryCallBack() {
            @Override
            public String getSql() {
                return "insert bin_log_file_logger(log_file_name, start_read_datetime, last_pointer) values(?, now(), ?)";
            }

            @Override
            public Object doInCallback(PreparedStatement pstmt) throws SQLException {
                pstmt.setString(1, nextFile);
                pstmt.setLong(2, newPos);
                return null;
            }
        });
    }

    private void processRowEvent(Event event) {
        RowEventProcessService.submitRowEvent(event);
    }

    private void processTableMapEvent(Event event) {
        TableMapEventData data = event.getData();
        TableMapContext.addTableMap(data.getTableID(), data);
        String schema = data.getDbName();
        String table = data.getTableName();
        if(ToiContext.contains(new ToiEntry(schema, table))) {
            EoiContext.addTable(data.getTableID());
        }
    }
}
