package org.mysqlmv.cd.logevent.parser.impl;

import org.mysqlmv.cd.logevent.eventdef.data.TableMapEventData;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Kelvin Li on 11/17/2014 2:46 PM.
 */
public class TableMapContext {
    private static Map<Long, TableMapEventData> mappedTables = new HashMap<Long, TableMapEventData>();

    private TableMapContext() {}

    public static void addTableMap(long tableId, TableMapEventData data) {
        mappedTables.put(tableId, data);
    }

    public static TableMapEventData getTableMap(long tableId) {
        return mappedTables.get(tableId);
    }
}
