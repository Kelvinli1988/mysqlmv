package org.mysqlmv.cd.logevent.parser.impl;

import org.mysqlmv.cd.logevent.eventdef.data.UpdateRowsIEventData;
import org.mysqlmv.common.io.ByteArrayInputStream;

import java.io.IOException;

/**
 * Created by Kelvin Li on 11/17/2014 5:33 PM.
 */
public class UpdateRowsEventDataParser extends AbstractRowsEventDataParser<UpdateRowsIEventData> {
    @Override
    public UpdateRowsIEventData parse(ByteArrayInputStream input) throws IOException {
        UpdateRowsIEventData data = new UpdateRowsIEventData();
        if(parseCommon(input, data)) {}
            data.setColumnUsageAfterUpdate(input.readBitSet(data.getColumnNum(), true));
            parseRows(input, data);
//        }
        return data;
    }
}
