package org.mysqlmv.etp.context;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by Kelvin Li on 12/3/2014 1:33 PM.
 */
public class ToiContext {
    private static ConcurrentMap<ToiEntry, Set<ToiValue>> toiContextMap = new ConcurrentHashMap<ToiEntry, Set<ToiValue>>();

    public synchronized static void addToiEntry(ToiEntry entry, ToiValue value) {
        Set<ToiValue> tvalues = toiContextMap.get(entry);
        if(tvalues == null) {
            tvalues = new HashSet<ToiValue>();
            toiContextMap.put(entry, tvalues);
        }
        tvalues.add(value);
    }

    public synchronized static void addToiEntry(Map.Entry<ToiEntry, Set<ToiValue>> entry) {
        toiContextMap.put(entry.getKey(), entry.getValue());
    }

    public static Set<ToiValue> getToiValue(ToiEntry entry) {
        Set<ToiValue> toRet = toiContextMap.get(entry);
        if(toRet == null) {
            return new HashSet<ToiValue>();
        }
        return toRet;
    }

    public static boolean contains(ToiEntry entry) {
        return toiContextMap.containsKey(entry);
    }
}
