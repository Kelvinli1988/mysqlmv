package org.mysqlmv.cd.logevent.parser.impl;

import org.mysqlmv.cd.logevent.eventdef.data.RotateEventData;
import org.mysqlmv.cd.logevent.parser.EventDataParser;
import org.mysqlmv.common.io.ByteArrayInputStream;

import java.io.IOException;

/**
 * Created by Kelvin Li on 11/18/2014 9:20 AM.
 */
public class RotateEventDataParser implements EventDataParser<RotateEventData> {
    @Override
    public RotateEventData parse(ByteArrayInputStream input) throws IOException {
        RotateEventData data = new RotateEventData();
        data.setPosOfNextLog(input.readLong(8));
        data.setNameOfNextLog(new String(input.read(input.available())));
        return data;
    }
}
