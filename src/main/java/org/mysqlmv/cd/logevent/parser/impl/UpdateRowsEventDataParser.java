package org.mysqlmv.cd.logevent.parser.impl;

import org.mysqlmv.cd.logevent.eventdef.data.UpdateRowsEventData;
import org.mysqlmv.cd.logevent.eventdef.data.WriteRowsEventData;
import org.mysqlmv.common.io.ByteArrayInputStream;

import java.io.IOException;

/**
 * Created by Kelvin Li on 11/17/2014 5:33 PM.
 */
public class UpdateRowsEventDataParser extends AbstractRowsEventDataParser<UpdateRowsEventData> {
    @Override
    public UpdateRowsEventData parse(ByteArrayInputStream input) throws IOException {
        UpdateRowsEventData data = new UpdateRowsEventData();
        if(parseCommon(input, data)) {
            data.setColumnUsageAfterUpdate(input.readBitSet(data.getColumnNum(), true));
            parseRows(input, data);
        }
        return data;
    }
}
