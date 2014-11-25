package org.mysqlmv.cd.logevent.parser.impl;

import org.mysqlmv.cd.logevent.eventdef.data.WriteRowsEventData;
import org.mysqlmv.common.io.ByteArrayInputStream;

import java.io.IOException;

/**
 * Created by Kelvin Li on 11/17/2014 1:13 PM.
 */
public class WriteRowsEventDataParser extends AbstractRowsEventDataParser<WriteRowsEventData> {

    @Override
    public WriteRowsEventData parse(ByteArrayInputStream input) throws IOException {
        WriteRowsEventData data = new WriteRowsEventData();
        parseCommon(input, data);
        parseRows(input, data);
        return data;
    }
}
