package org.mysqlmv.cd.logevent.parser;

import org.mysqlmv.cd.logevent.Event;
import org.mysqlmv.cd.logevent.EventMiner;
import org.mysqlmv.cd.logevent.LogEventType;
import org.mysqlmv.cd.logevent.eventdef.data.BinaryEventData;
import org.mysqlmv.cd.logevent.eventdef.data.QueryEventData;
import org.mysqlmv.cd.logevent.parser.impl.QueryEventDataParser;
import org.mysqlmv.common.io.ByteArrayInputStream;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * Created by Kelvin Li on 11/14/2014 4:07 PM.
 */
public class QueryEventDataParserTest {
    Event event;
    @BeforeClass
    public void prepare() throws IOException {
        EventMiner.getINSTANCE().switchFile("src/test/resources/PVGN50874064A-bin.000001", 4L);
        // skip the first event, it is a format description event;
        EventMiner.getINSTANCE().next();
        event = EventMiner.getINSTANCE().next();
    }

    @Test
    public void testParse() throws IOException {
        Assert.assertEquals(event.getHeader().getEventType(), LogEventType.QUERY);
        Assert.assertEquals(event.isRawData(), true);
        if(event.isRawData()) {
            // parse the data;
            BinaryEventData eData = event.getData();
            QueryEventDataParser parser = new QueryEventDataParser();
            QueryEventData data =  parser.parse(new ByteArrayInputStream(new java.io.ByteArrayInputStream(eData.getData())));
            Assert.assertEquals(data.getErrCode(), 0);
            Assert.assertEquals(data.getStatusVariableBlockLength(), 26);
            Assert.assertEquals(data.getDefaultDBName(), "mysql");
            Assert.assertEquals(data.getSqlStr(), "BEGIN");
            Assert.assertEquals(data.getDbNameLength(), 5);
        }
    }
}
