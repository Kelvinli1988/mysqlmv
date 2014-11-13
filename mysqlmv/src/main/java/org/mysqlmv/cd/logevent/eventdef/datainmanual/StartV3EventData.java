package org.mysqlmv.cd.logevent.eventdef.datainmanual;

import org.mysqlmv.cd.logevent.EventData;

/**
 * Created by Kelvin Li on 11/13/2014 2:06 PM.
 */
public class StartV3EventData implements EventData {
    /*
    +=========================+
    |  Fixed data part        |
    +=========================+
    */
    /**
     * 2 bytes.The binary log format version. This is 1 in MySQL 3.23 and 3 in MySQL 4.0 and 4.1.
     * (In MySQL 5.0 and up, FORMAT_DESCRIPTION_EVENT is used instead of START_EVENT_V3.)
     */
    private int binLogFormatVersion;
    /**
     * 50bytes, end with 0x00
     */
    private String serverVersion;
    /**
     * 4bytes. Timestamp in seconds when this event was created (this is the moment
     * when the binary log was created). This value is redundant; the same value
     * occurs in the timestamp header field.
     */
    private long timestamp;
    /*
    +=========================+
    |  Variable data part     |
    +=========================+
     */
    // Empty


    public int getBinLogFormatVersion() {
        return binLogFormatVersion;
    }

    public void setBinLogFormatVersion(int binLogFormatVersion) {
        this.binLogFormatVersion = binLogFormatVersion;
    }

    public String getServerVersion() {
        return serverVersion;
    }

    public void setServerVersion(String serverVersion) {
        this.serverVersion = serverVersion;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
