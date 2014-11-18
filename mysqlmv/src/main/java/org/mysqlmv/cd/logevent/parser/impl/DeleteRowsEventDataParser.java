package org.mysqlmv.cd.logevent.parser.impl;

import org.mysqlmv.cd.logevent.eventdef.data.DeleteRowsEventData;
import org.mysqlmv.cd.logevent.eventdef.data.UpdateRowsEventData;
import org.mysqlmv.cd.logevent.eventdef.data.WriteRowsEventData;
import org.mysqlmv.common.io.ByteArrayInputStream;

import java.io.IOException;

/**
 * Created by Kelvin Li on 11/17/2014 5:42 PM.
 */
public class DeleteRowsEventDataParser extends AbstractRowsEventDataParser<DeleteRowsEventData> {
    @Override
    public DeleteRowsEventData parse(ByteArrayInputStream input) throws IOException {
        DeleteRowsEventData data = new DeleteRowsEventData();
        parseCommon(input, data);
        parseRows(input, data);
        return data;
    }
}
