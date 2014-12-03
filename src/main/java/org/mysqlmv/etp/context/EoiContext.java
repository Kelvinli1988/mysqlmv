package org.mysqlmv.etp.context;

import org.mysqlmv.cd.logevent.Event;
import org.mysqlmv.cd.logevent.eventdef.data.RowsEventData;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Created by Kelvin Li on 12/3/2014 4:16 PM.
 */
public class EoiContext {
    private static Set<Long> tableIdSet = new ConcurrentSkipListSet<Long>();

    public static void addTable(long tableId) {
        tableIdSet.add(tableId);
    }

    public static  boolean isEoi(Event event) {
        RowsEventData data = event.getData();
        return tableIdSet.contains(data.getTableId());
    }
}