package org.mysqlmv.cd.logevent.parser.impl;

import org.mysqlmv.cd.logevent.EventData;
import org.mysqlmv.cd.logevent.eventdef.data.RowsEventData;
import org.mysqlmv.cd.logevent.parser.EventDataParser;
import org.mysqlmv.common.io.ByteArrayInputStream;

/**
 * Created by Kelvin Li on 11/13/2014 1:55 PM.
 */
public class AbstractEventDataParser<T extends RowsEventData> implements EventDataParser<T> {
    @Override
    public T parse(ByteArrayInputStream input) {
        return null;
    }
}
