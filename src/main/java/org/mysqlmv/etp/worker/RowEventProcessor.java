package org.mysqlmv.etp.worker;

import org.mysqlmv.cd.logevent.Event;
import org.mysqlmv.cd.logevent.eventdef.data.*;
import org.mysqlmv.cd.logevent.parser.impl.TableMapContext;
import org.mysqlmv.etp.context.ToiContext;
import org.mysqlmv.etp.context.ToiEntry;
import org.mysqlmv.etp.context.ToiValue;
import org.mysqlmv.etp.dao.EtpDao;
import org.slf4j.Logger;

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
        RowsIEventData data = (RowsIEventData)rowEvent.getData();
        TableMapIEventData tMap = TableMapContext.getTableMap(data.getTableId());
        schema = tMap.getDbName();
        table = tMap.getTableName();
        int ordinal = EtpDao.findPKOrdinal(schema, table);
        List<RowsIEventData.Row> rows = data.getRows();
        if(data instanceof DeleteRowsIEventData) {
            processDelete(ordinal);
        } else if(data instanceof WriteRowsIEventData) {
            processInsert(ordinal);
        } else if(data instanceof UpdateRowsIEventData) {
            processUpdate(ordinal);
        }
    }

    private void processInsert(int idOrdinal) throws SQLException {
        RowsIEventData data = (RowsIEventData)rowEvent.getData();
        Set<ToiValue> vValueList = ToiContext.getToiValue(new ToiEntry(schema, table));
        for(RowsIEventData.Row row: data.getRows()) {
            for(ToiValue toiValue : vValueList) {
                Object idObj = row.getCells().get(idOrdinal - 1).getValue();
                int id = 0;//Integer);
                if(idObj instanceof Integer) {
                    id = (Integer) idObj;
                } else if(idObj instanceof Long) {
                    id = ((Long)idObj).intValue();
                }
                int toiId = toiValue.getMviewToiId();
                EtpDao.insertTOI(schema, table, id, toiId, RowOperation.INSERT);
            }
        }
    }

    private void processDelete(int idOrdinal) throws SQLException {
        RowsIEventData data = (RowsIEventData)rowEvent.getData();
        Set<ToiValue> vValueList = ToiContext.getToiValue(new ToiEntry(schema, table));
        for(RowsIEventData.Row row: data.getRows()) {
            for(ToiValue toiValue : vValueList) {
                int id = (Integer)row.getCells().get(idOrdinal - 1).getValue();
                int toiId = toiValue.getMviewToiId();
                EtpDao.insertTOI(schema, table, id, toiId, RowOperation.DELETE);
            }

        }
    }

    private void processUpdate(int idOrdinal) throws SQLException {
        RowsIEventData data = (RowsIEventData)rowEvent.getData();
        Set<ToiValue> tValueList = ToiContext.getToiValue(new ToiEntry(schema, table));
        for(int i=0; i<data.getRows().size();) {
            int id = (Integer) data.getRows().get(i).getCells().get(idOrdinal - 1).getValue();
            for (ToiValue toiValue : tValueList) {
                int toiId = toiValue.getMviewToiId();
                EtpDao.insertTOI(schema, table, id, toiId, RowOperation.UPDATE_D);
            }
            i++;
            for (ToiValue toiValue : tValueList) {
                int toiId = toiValue.getMviewToiId();
                EtpDao.insertTOI(schema, table, id, toiId, RowOperation.UPDATE_I);
            }
            i++;
        }
    }

    public RowEventProcessor(Event rowEvent) {
        this.rowEvent = rowEvent;
    }
}
