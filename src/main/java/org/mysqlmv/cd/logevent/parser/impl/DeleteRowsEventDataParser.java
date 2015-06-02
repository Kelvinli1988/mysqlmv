package org.mysqlmv.cd.logevent.parser.impl;

import org.mysqlmv.cd.logevent.eventdef.data.DeleteRowsIEventData;
import org.mysqlmv.common.io.ByteArrayInputStream;

import java.io.IOException;

/**
 * Created by Kelvin Li on 11/17/2014 5:42 PM.
 */
public class DeleteRowsEventDataParser extends AbstractRowsEventDataParser<DeleteRowsIEventData> {
    @Override
    public DeleteRowsIEventData parse(ByteArrayInputStream input) throws IOException {
        DeleteRowsIEventData data = new DeleteRowsIEventData();
        if(parseCommon(input, data)) {
            parseRows(input, data);
        }

        return data;
    }
}
