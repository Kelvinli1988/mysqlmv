package org.mysqlmv.cd.logevent.eventdef.header;

/**
 * Created by Kelvin Li on 11/13/2014 10:22 AM.
 */
public class EventHeaderV1 extends AbstractEventHeader {

    public long getNextPosition() {
        throw new UnsupportedOperationException();
    }

    public void setNextPosition(long nextPosition) {
        throw new UnsupportedOperationException();
    }

    public int getFlag() {
        throw new UnsupportedOperationException();
    }

    public void setFlag(int flag) {
        throw new UnsupportedOperationException();
    }

    public byte[] getExtraHeader() {
        throw new UnsupportedOperationException();
    }

    public void setExtraHeader(byte[] extraHeader) {
        throw new UnsupportedOperationException();
    }

    public EventVersion getVersion() {
        return EventVersion.V_1;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("EventHeaderV1");
        sb.append("{timestamp=").append(getTimestamp());
        sb.append(", eventType=").append(getEventType().toString());
        sb.append(", serverId=").append(getServerId());
        sb.append(", headerLength=").append(getHeaderLength());
        sb.append(", dataLength=").append(getDataLength());
        sb.append(", nextPosition=").append(getNextPosition());
        sb.append(", flags=").append(getFlag());
        sb.append('}');
        return sb.toString();
    }
}
