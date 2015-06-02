package org.mysqlmv.cd.logevent.parser.impl;

import org.mysqlmv.cd.logevent.eventdef.data.TableMapIEventData;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Kelvin Li on 11/17/2014 2:46 PM.
 */
public class TableMapContext {
    private static Map<Long, TableMapIEventData> mappedTables = new HashMap<Long, TableMapIEventData>();

    private TableMapContext() {}

    public static void addTableMap(long tableId, TableMapIEventData data) {
        mappedTables.put(tableId, data);
    }

    public static TableMapIEventData getTableMap(long tableId) {
        return mappedTables.get(tableId);
    }
}
