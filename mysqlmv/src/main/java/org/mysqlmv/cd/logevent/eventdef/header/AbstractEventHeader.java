package org.mysqlmv.cd.logevent.eventdef.header;

import org.mysqlmv.cd.logevent.EventHeader;
import org.mysqlmv.cd.logevent.LogEventType;

/**
 * Created by Kelvin Li on 11/13/2014 10:25 AM.
 */
public abstract class AbstractEventHeader implements EventHeader {
    // v1 (MySQL 3.23)
    /**
     * 4 bytes. This is the time at which the statement began executing.
     * It is represented as the number of seconds since 1970 (UTC), like the TIMESTAMP SQL data type.
     */
    private long timestamp;
    /**
     * 1 byte. The type of event. 1 means START_EVENT_V3, 2 means QUERY_EVENT, and so forth.
     * These numbers are defined in the enum Log_event_type enumeration in log_event.h.
     * (See Event Classes and Types.)
     */
    private LogEventType eventType;
    /**
     * 4 bytes. The ID of the mysqld server that originally created the event.
     */
    private long serverId;
    /**
     * 4 bytes. The total size of this event. This includes both the header and data parts.
     * Most events are less than 1000 bytes, except when using LOAD DATA INFILE (where events
     * contain the loaded file, so they can be big).
     */
    private long eventLength;
    // v3 (MySQL 4.0.2-4.1)
    private long nextPosition;
    /**
     * 2 bytes. The possible flag values are described at Event Flags.
     */
    private int flag;
    // V4 (MYSQL 5.0+) usually it is empty.
    /**
     * Variable-sized. The size of this field is determined by the format description event
     * that occurs as the first event in the file. Currently, the size is 0, so, in effect,
     * this field never actually occurs in any event. At such time as the size becomes nonzero,
     * this field still will not appear in events of type FORMAT_DESCRIPTION_EVENT or ROTATE_EVENT.
     */
    private byte[] extraHeader;

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public LogEventType getEventType() {
        return eventType;
    }

    public void setEventType(LogEventType eventType) {
        this.eventType = eventType;
    }

    public long getServerId() {
        return serverId;
    }

    public void setServerId(long serverId) {
        this.serverId = serverId;
    }

    public long getEventLength() {
        return eventLength;
    }

    public void setEventLength(long eventLength) {
        this.eventLength = eventLength;
    }

    public long getNextPosition() {
        return nextPosition;
    }

    public void setNextPosition(long nextPosition) {
        this.nextPosition = nextPosition;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public byte[] getExtraHeader() {
        return extraHeader;
    }

    public void setExtraHeader(byte[] extraHeader) {
        this.extraHeader = extraHeader;
    }

    public EventVersion getVersion() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getHeaderLength() {
        return 19;
    }

    @Override
    public long getDataLength() {
        return getEventLength() - getHeaderLength();
    }
}
