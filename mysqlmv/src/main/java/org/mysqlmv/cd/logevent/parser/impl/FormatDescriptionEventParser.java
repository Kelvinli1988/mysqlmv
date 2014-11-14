package org.mysqlmv.cd.logevent.parser.impl;

import org.mysqlmv.cd.logevent.EventData;
import org.mysqlmv.cd.logevent.eventdef.datainmanual.FormatDescriptionEventData;
import org.mysqlmv.cd.logevent.parser.EventDataParser;
import org.mysqlmv.common.io.ByteArrayInputStream;

import java.io.IOException;

/**
 * Created by Kelvin Li on 11/14/2014 3:11 PM.
 */
public class FormatDescriptionEventParser implements EventDataParser<FormatDescriptionEventData> {
    @Override
    public FormatDescriptionEventData parse(ByteArrayInputStream stream) throws IOException {
        FormatDescriptionEventData eData = new FormatDescriptionEventData();
        eData.setLogVersion(stream.readInteger(2));
        eData.setServerVersion(stream.readZeroTerminatedString(50));
        stream.skip(4); // redundant, present in a header
        eData.setHeaderLength(stream.readInteger(1));
        // lengths for all event types
        return eData;
    }
}
