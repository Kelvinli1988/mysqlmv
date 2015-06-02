package org.mysqlmv.cd.logevent;

import org.mysqlmv.cd.logevent.eventdef.data.BinaryIEventData;

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
public class Event implements IEvent {

    private final IEventHeader header;
    private final IEventData data;

    public Event(IEventHeader header, IEventData data) {
        this.header = header;
        this.data = data;
    }

    public IEventHeader getHeader() {
        return header;
    }

    public IEventData getData() {
        return data;
    }

    public boolean isRawData() {
        return data instanceof BinaryIEventData;
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
