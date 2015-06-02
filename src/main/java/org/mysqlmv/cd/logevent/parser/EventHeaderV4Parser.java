package org.mysqlmv.cd.logevent.parser;

import org.mysqlmv.cd.logevent.LogEventType;
import org.mysqlmv.cd.logevent.eventdef.header.IEventHeaderV4;
import org.mysqlmv.common.io.ByteArrayInputStream;

import java.io.IOException;

/**
 * Created by Kelvin Li on 11/13/2014 1:43 PM.
 */
public class EventHeaderV4Parser implements EventHeaderParser<IEventHeaderV4> {
    private static final LogEventType[] EVENT_TYPES = LogEventType.values();

    @Override
    public IEventHeaderV4 parse(ByteArrayInputStream input) throws IOException {
        IEventHeaderV4 header = new IEventHeaderV4();
        header.setTimestamp(input.readLong(4));
        header.setEventType(getEventType(input.readInteger(1)));
        header.setServerId(input.readLong(4));
        header.setEventLength(input.readInteger(4));
        header.setNextPosition(input.readLong(4));
        header.setFlag(input.readInteger(2));
        return header;
    }

    private static LogEventType getEventType(int ordinal) throws IOException {
        if (ordinal >= EVENT_TYPES.length) {
            throw new IOException("Unknown event type " + ordinal);
        }
        return EVENT_TYPES[ordinal];
    }
}
