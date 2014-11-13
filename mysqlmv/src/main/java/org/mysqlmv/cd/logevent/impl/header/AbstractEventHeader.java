package org.mysqlmv.cd.logevent.impl.header;

import org.mysqlmv.cd.logevent.EventHeader;
import org.mysqlmv.cd.logevent.LogEventType;

/**
 * Created by Kelvin Li on 11/13/2014 10:25 AM.
 */
public abstract class AbstractEventHeader implements EventHeader {
    // v1 (MySQL 3.23)
    private long timestamp;
    private LogEventType eventType;
    private long serverId;
    private long eventLength;
    // v3 (MySQL 4.0.2-4.1)
    private long nextPosition;
    private int flag;
    // V4 (MYSQL 5.0+) usually it is empty.
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
