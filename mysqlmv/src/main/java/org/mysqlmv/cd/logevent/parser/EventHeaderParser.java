package org.mysqlmv.cd.logevent.parser;

import org.mysqlmv.cd.logevent.EventHeader;

/**
 * Created by Kelvin Li on 11/13/2014 10:53 AM.
 */
public interface EventHeaderParser<T extends EventHeader> {
    T parse(byte[] stream);
}
