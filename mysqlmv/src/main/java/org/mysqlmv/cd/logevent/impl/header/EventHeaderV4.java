package org.mysqlmv.cd.logevent.impl.header;

/**
 * Created by Kelvin Li on 11/13/2014 10:34 AM.
 */
public class EventHeaderV4 extends AbstractEventHeader {
    public EventVersion getVersion() {
        return EventVersion.V_4;
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
        sb.append('}');
        return sb.toString();
    }
}
