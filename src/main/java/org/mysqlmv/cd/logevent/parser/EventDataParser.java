package org.mysqlmv.cd.logevent.parser;

import org.mysqlmv.cd.logevent.IEventData;
import org.mysqlmv.common.io.ByteArrayInputStream;

import java.io.IOException;

/**
 * Created by Kelvin Li on 11/13/2014 10:54 AM.
 */
public interface EventDataParser<T extends IEventData> {
    T parse(ByteArrayInputStream input) throws IOException;
}
