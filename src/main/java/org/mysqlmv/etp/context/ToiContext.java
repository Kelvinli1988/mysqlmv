package org.mysqlmv.etp.context;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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

    public static Set<ToiValue> getToiValue(ToiEntry entry) {
        Set<ToiValue> toRet = toiContextMap.get(entry);
        if(toRet == null) {
            return new HashSet<ToiValue>();
        }
        return toRet;
    }
}
