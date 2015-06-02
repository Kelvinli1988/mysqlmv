package org.mysqlmv.cd.logevent.processors;

import org.mysqlmv.cd.dao.CdDao;
import org.mysqlmv.cd.logevent.Event;
import org.mysqlmv.cd.logevent.EventProcessor;
import org.mysqlmv.cd.logevent.LogEventType;
import org.mysqlmv.cd.logevent.eventdef.data.RotateIEventData;
import org.mysqlmv.cd.logevent.eventdef.data.RowsIEventData;
import org.mysqlmv.cd.logevent.eventdef.data.TableMapIEventData;
import org.mysqlmv.cd.logevent.parser.impl.TableMapContext;
import org.mysqlmv.etp.context.EoiContext;
import org.mysqlmv.etp.context.ToiContext;
import org.mysqlmv.etp.context.ToiEntry;
import org.mysqlmv.etp.worker.RowEventProcessService;

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
        } else if(event.getData() instanceof RowsIEventData) {
            if(EoiContext.isEoi(((RowsIEventData) event.getData()).getTableId())) {
                processRowEvent(event);
            }
        } else if(event.getHeader().getEventType().equals(LogEventType.TABLE_MAP)) {
            processTableMapEvent(event);
        }
    }

    private void processRotateEvent(Event event) throws SQLException {
        RotateIEventData rdata = (RotateIEventData)event.getData();
        final String nextFile = rdata.getNameOfNextLog();
        final long newPos = rdata.getPosOfNextLog();
        final long eventTime = event.getHeader().getTimestamp();
        // 1. update rotate time.
        CdDao.updateLogFileRotate(eventTime);
        // 2. insert new record log.
        CdDao.insertNewFileStatus(nextFile, newPos);
    }

    private void processRowEvent(Event event) {
        RowEventProcessService.submitRowEvent(event);
    }

    private void processTableMapEvent(Event event) {
        TableMapIEventData data = (TableMapIEventData)event.getData();
        TableMapContext.addTableMap(data.getTableID(), data);
        String schema = data.getDbName();
        String table = data.getTableName();
        if(ToiContext.contains(new ToiEntry(schema, table))) {
            EoiContext.addTable(data.getTableID());
        }
    }
}
