package org.mysqlmv.cd.logevent;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Kelvin Li on 11/14/2014 4:42 PM.
 */
public class RecognizedEventType {
    private static  List<LogEventType> recognized = new ArrayList<LogEventType>();

    static {
        recognized.add(LogEventType.FORMAT_DESCRIPTION);
        recognized.add(LogEventType.QUERY);
        recognized.add(LogEventType.XID);
        recognized.add(LogEventType.TABLE_MAP);
        recognized.add(LogEventType.WRITE_ROWS);
        recognized.add(LogEventType.UPDATE_ROWS);
        recognized.add(LogEventType.DELETE_ROWS);
        recognized.add(LogEventType.ROTATE);
    }

    public static boolean isRecognized(LogEventType type) {
        return recognized.contains(type);
    }
}
