package org.mysqlmv.cd.logevent.parser.impl;

import org.mysqlmv.cd.logevent.eventdef.data.FormatDescriptionIEventData;
import org.mysqlmv.cd.logevent.parser.EventDataParser;
import org.mysqlmv.common.io.ByteArrayInputStream;

import java.io.IOException;

/**
 * Created by Kelvin Li on 11/14/2014 3:11 PM.
 */
public class FormatDescriptionEventParser implements EventDataParser<FormatDescriptionIEventData> {
    @Override
    public FormatDescriptionIEventData parse(ByteArrayInputStream stream) throws IOException {
        FormatDescriptionIEventData eData = new FormatDescriptionIEventData();
        eData.setLogVersion(stream.readInteger(2));
        eData.setServerVersion(stream.readZeroTerminatedString(50));
        stream.skip(4); // redundant, present in a header
        eData.setHeaderLength(stream.readInteger(1));
        // lengths for all event types
        return eData;
    }
}
