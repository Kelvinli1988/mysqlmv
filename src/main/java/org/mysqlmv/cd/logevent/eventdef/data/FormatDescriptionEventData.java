package org.mysqlmv.cd.logevent.eventdef.data;

import org.mysqlmv.cd.logevent.EventData;

/**
 * Created by Kelvin Li on 11/13/2014 4:43 PM.
 */

/**
 * This event occurs at the beginning of v4 binary log files. See Binary Log Versions for how it is used.
 * <p/>
 * In MySQL 5.0 and up, all binary log files start with a FORMAT_DESCRIPTION_EVENT, but there will be a way to distinguish between a FORMAT_DESCRIPTION_EVENT created at mysqld startup and other FORMAT_DESCRIPTION_EVENT instances; such a distinction is needed because the first category of FORMAT_DESCRIPTION_EVENT (which means the master has started) should trigger some cleaning tasks on the slave. (Suppose the master died brutally and restarted: the slave must delete old replicated temporary tables.)
 */
public class FormatDescriptionEventData implements EventData {
    /*
    +=========================+
    |  Fixed data part        |
    +=========================+
    */
    /**
     * 2 bytes. The binary log format version. This is 4 in MySQL 5.0 and up.
     */
    private int logVersion;
    /**
     * 50 bytes. The MySQL server's version (example: 5.0.14-debug-log), padded with 0x00 bytes on the right.
     */
    private String serverVersion;
    /**
     * 4 bytes. Timestamp in seconds when this event was created (this is the moment when the binary log was created). This value is redundant; the same value occurs in the timestamp header field.
     */
    private long timestamp;
    /**
     * 1 byte. The header length. This length - 19 gives the size of the extra headers field at the end of the header for other events.
     */
    private int headerLength;
    /**
     * Variable-sized. An array that indicates the post-header lengths for all event types. There is one byte per event type that the server knows about.
     */
    // TODO what is this?
    private byte[] postHeaderLength;
    /*
    +=========================+
    |  Variable data part     |
    +=========================+
     */
    // Empty


    public int getLogVersion() {
        return logVersion;
    }

    public void setLogVersion(int logVersion) {
        this.logVersion = logVersion;
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

    public int getHeaderLength() {
        return headerLength;
    }

    public void setHeaderLength(int headerLength) {
        this.headerLength = headerLength;
    }

    public byte[] getPostHeaderLength() {
        return postHeaderLength;
    }

    public void setPostHeaderLength(byte[] postHeaderLength) {
        this.postHeaderLength = postHeaderLength;
    }
}
