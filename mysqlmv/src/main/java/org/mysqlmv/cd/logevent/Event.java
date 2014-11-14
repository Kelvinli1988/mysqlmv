package org.mysqlmv.cd.logevent;

import org.mysqlmv.cd.logevent.eventdef.datainmanual.BinaryEventData;

import java.io.Serializable;

/**
 * Created by Kelvin Li on 11/13/2014 10:47 AM.
 */

/**
 * All events have a common general structure consisting of an event header followed by event data:
 * <p/>
 * +===================+
 * | event header      |
 * +===================+
 * | event data        |
 * +===================+
 */
public class Event implements Serializable {

    private EventHeader header;
    private EventData data;

    public Event() {
    }

    public Event(EventHeader header, EventData data) {
        this.header = header;
        this.data = data;
    }

    public <T extends EventHeader> T getHeader() {
        return (T) header;
    }

    public <V extends EventData> V getData() {
        return (V) data;
    }

    public boolean isRawData() {
        return data instanceof BinaryEventData;
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
