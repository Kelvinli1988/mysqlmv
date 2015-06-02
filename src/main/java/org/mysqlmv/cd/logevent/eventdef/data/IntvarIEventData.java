package org.mysqlmv.cd.logevent.eventdef.data;

import org.mysqlmv.cd.logevent.IEventData;

/**
 * Created by Kelvin Li on 11/13/2014 2:57 PM.
 */
public class IntvarIEventData implements IEventData {
    /*
    +=========================+
    |  Fixed data part        |
    +=========================+
    */
    // Empty

    /*
    +=========================+
    |  Variable data part     |
    +=========================+
     */
    /**
     * 1 byte. A value indicating the variable type: LAST_INSERT_ID_EVENT = 1 or INSERT_ID_EVENT = 2.
     */
    private int indicator;
    /**
     * 8 bytes. An unsigned integer indicating the value to be used for the LAST_INSERT_ID() invocation or AUTO_INCREMENT column.
     */
    private long value;

    public int getIndicator() {
        return indicator;
    }

    public void setIndicator(int indicator) {
        this.indicator = indicator;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }
}
