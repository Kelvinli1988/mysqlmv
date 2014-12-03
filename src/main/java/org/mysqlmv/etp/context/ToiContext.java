package org.mysqlmv.etp.context;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by Kelvin Li on 12/3/2014 1:33 PM.
 */
public class ToiContext {
    private static ConcurrentMap<ToiEntry, List<ToiValue>> toiContextMap = new ConcurrentHashMap<ToiEntry, List<ToiValue>>();

    public synchronized static void putToiEntry(ToiEntry entry, ToiValue value) {
        List<ToiValue> tvalues = toiContextMap.get(entry);
        if(tvalues == null) {
            tvalues = new ArrayList<ToiValue>();
            toiContextMap.put(entry, tvalues);
        }
        tvalues.add(value);
    }

    public static List<ToiValue> getToiValue(ToiEntry entry) {
        List<ToiValue> toRet = toiContextMap.get(entry);
        if(toRet == null) {
            return new ArrayList<ToiValue>();
        }
        return toRet;
    }
}
