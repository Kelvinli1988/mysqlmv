package org.mysqlmv.cd.logevent.parser.impl;

import org.mysqlmv.cd.logevent.eventdef.data.WriteRowsIEventData;
import org.mysqlmv.common.io.ByteArrayInputStream;

import java.io.IOException;

/**
 * Created by Kelvin Li on 11/17/2014 1:13 PM.
 */
public class WriteRowsEventDataParser extends AbstractRowsEventDataParser<WriteRowsIEventData> {

    @Override
    public WriteRowsIEventData parse(ByteArrayInputStream input) throws IOException {
        WriteRowsIEventData data = new WriteRowsIEventData();
        if(parseCommon(input, data)) {
            parseRows(input, data);
        }
        return data;
    }
}
