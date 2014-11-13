package org.mysqlmv.cd.logevent.parser;

import org.mysqlmv.cd.logevent.EventData;
import org.mysqlmv.common.io.ByteArrayInputStream;

/**
 * Created by Kelvin Li on 11/13/2014 1:55 PM.
 */
public class AbstractEventDataParser implements EventDataParser<EventData> {
    @Override
    public EventData parse(ByteArrayInputStream input) {
        return null;
    }
}
