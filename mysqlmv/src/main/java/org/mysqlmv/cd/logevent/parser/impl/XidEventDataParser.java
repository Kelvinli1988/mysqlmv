package org.mysqlmv.cd.logevent.parser.impl;

import org.mysqlmv.cd.logevent.eventdef.datainmanual.XidEventData;
import org.mysqlmv.cd.logevent.parser.EventDataParser;
import org.mysqlmv.common.io.ByteArrayInputStream;

import java.io.IOException;

/**
 * Created by Kelvin Li on 11/14/2014 4:49 PM.
 */
public class XidEventDataParser implements EventDataParser<XidEventData> {
    @Override
    public XidEventData parse(ByteArrayInputStream stream) throws IOException {
        XidEventData edata = new XidEventData();
        edata.setXid(stream.readLong(8));
        return edata;
    }
}
