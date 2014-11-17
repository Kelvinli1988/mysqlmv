package org.mysqlmv.cd.logevent.eventdef.data;

import org.mysqlmv.cd.logevent.EventData;

/**
 * Created by Kelvin Li on 11/13/2014 2:54 PM.
 */
public class RotateEventData implements EventData {
    /*
    +=========================+
    |  Fixed data part        |
    +=========================+
    */

    /**
     * The position of the first event in the next log file. Always contains the number 4
     * (meaning the next event starts at position 4 in the next binary log).
     * This field is not present in v1; presumably the value is assumed to be 4.
     */
    private long posOfNextLog;

    /*
    +=========================+
    |  Variable data part     |
    +=========================+
     */
    /**
     * The name of the next binary log. The filename is not null-terminated.
     * Its length is the event size minus the size of the fixed parts.
     */
    private String nameOfNextLog;

    public long getPosOfNextLog() {
        return posOfNextLog;
    }

    public void setPosOfNextLog(long posOfNextLog) {
        this.posOfNextLog = posOfNextLog;
    }

    public String getNameOfNextLog() {
        return nameOfNextLog;
    }

    public void setNameOfNextLog(String nameOfNextLog) {
        this.nameOfNextLog = nameOfNextLog;
    }
}
