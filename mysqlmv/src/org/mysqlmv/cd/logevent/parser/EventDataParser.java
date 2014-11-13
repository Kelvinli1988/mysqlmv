package org.mysqlmv.cd.logevent.parser;

import org.mysqlmv.cd.logevent.EventData;

/**
 * Created by Kelvin Li on 11/13/2014 10:54 AM.
 */
public interface EventDataParser <T extends EventData> {
    T parse(byte[] stream);
}
