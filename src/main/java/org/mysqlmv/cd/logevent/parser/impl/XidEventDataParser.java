package org.mysqlmv.cd.logevent.parser.impl;

import org.mysqlmv.cd.logevent.eventdef.data.XidIEventData;
import org.mysqlmv.cd.logevent.parser.EventDataParser;
import org.mysqlmv.common.io.ByteArrayInputStream;

import java.io.IOException;

/**
 * Created by Kelvin Li on 11/14/2014 4:49 PM.
 */
public class XidEventDataParser implements EventDataParser<XidIEventData> {
    @Override
    public XidIEventData parse(ByteArrayInputStream stream) throws IOException {
        XidIEventData edata = new XidIEventData();
        edata.setXid(stream.readLong(8));
        return edata;
    }
}
