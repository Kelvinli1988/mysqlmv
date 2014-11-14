package org.mysqlmv.cd.logevent.parser.impl;

import org.mysqlmv.cd.logevent.eventdef.datainmanual.QueryEventData;
import org.mysqlmv.cd.logevent.parser.EventDataParser;
import org.mysqlmv.common.io.ByteArrayInputStream;

import java.io.IOException;

/**
 * Created by Kelvin Li on 11/14/2014 3:58 PM.
 */
public class QueryEventDataParser implements EventDataParser<QueryEventData> {
    @Override
    public QueryEventData parse(ByteArrayInputStream stream) throws IOException {
        QueryEventData eData = new QueryEventData();
        eData.setThreadId(stream.readLong(4));
        eData.setTimestamp(stream.readLong(4) * 1000L);
        eData.setDbNameLength(stream.readInteger(1));
        eData.setErrCode(stream.readInteger(2));
        eData.setStatusVariableBlockLength(stream.readInteger(2));
        // TODO these status variable need to parse, but do not need now.
        stream.skip(eData.getStatusVariableBlockLength());
        eData.setDefaultDBName(stream.readZeroTerminatedString());
        eData.setSqlStr(stream.readString(stream.available()));
        return eData;
    }
}
