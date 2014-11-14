package org.mysqlmv.cd.logevent.parser;

import org.mysqlmv.cd.logevent.BinLogFile;
import org.mysqlmv.cd.logevent.Event;
import org.mysqlmv.cd.logevent.LogEventType;
import org.mysqlmv.cd.logevent.eventdef.datainmanual.BinaryEventData;
import org.mysqlmv.cd.logevent.eventdef.datainmanual.FormatDescriptionEventData;
import org.mysqlmv.cd.logevent.eventdef.datainmanual.QueryEventData;
import org.mysqlmv.cd.logevent.parser.impl.FormatDescriptionEventParser;
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
    Event qEvent;
    @BeforeClass
    public void prepare() throws IOException {
        BinLogFile logFile = new BinLogFile("src/test/resources/PVGN50874064A-bin.000001");
        EventMiner.switchFile(logFile, 4L);
        // skip the first event, it is a format description event;
        EventMiner.nextEvent();
        qEvent = EventMiner.nextEvent();
        qEvent = EventMiner.nextEvent();
    }

    @Test
    public void testParse() throws IOException {
        Assert.assertEquals(qEvent.getHeader().getEventType(), LogEventType.QUERY);
        Assert.assertEquals(qEvent.isRawData(), true);
        if(qEvent.isRawData()) {
            // parse the data;
            BinaryEventData eData = qEvent.getData();
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
