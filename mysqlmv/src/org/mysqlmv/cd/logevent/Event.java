package org.mysqlmv.cd.logevent;

import java.io.Serializable;

/**
 * Created by Kelvin Li on 11/13/2014 10:47 AM.
 */
public class Event implements Serializable {
    private EventHeader header;
    private EventData data;

    public Event(EventHeader header, EventData data) {
        this.header = header;
        this.data = data;
    }

    public <T extends EventHeader> T getHeader() {
        return (T) header;
    }

    public <T extends EventData> T getData() {
        return (T) data;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Event");
        sb.append("{header=").append(header);
        sb.append(", data=").append(data);
        sb.append('}');
        return sb.toString();
    }
}
