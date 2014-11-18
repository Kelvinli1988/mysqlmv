package org.mysqlmv.cd.logevent.parser;

import org.mysqlmv.cd.logevent.BinLogFile;
import org.mysqlmv.cd.logevent.Event;
import org.mysqlmv.cd.logevent.LogEventType;
import org.mysqlmv.cd.logevent.eventdef.data.BinaryEventData;
import org.mysqlmv.cd.logevent.eventdef.data.TableMapEventData;
import org.mysqlmv.cd.logevent.parser.impl.TableMapEventDataParser;
import org.mysqlmv.common.io.ByteArrayInputStream;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * Created by Kelvin Li on 11/17/2014 9:32 AM.
 */
public class TableMapEventDataParserTest {
    Event event;
    @BeforeClass
    public void switchFile() throws IOException {
        EventMiner.getINSTANCE().switchFile("src/test/resources/PVGN50874064A-bin.000003", 4L);
        for(int i=0; ; i++) {
            Event ee = EventMiner.getINSTANCE().next();
            if(i == 2) {
                event = ee;
                break;
            }
        }
    }

    @Test
    public void parse() throws IOException {
        Assert.assertEquals(event.getHeader().getEventType(), LogEventType.TABLE_MAP);
        Assert.assertEquals(event.isRawData(), true);
        if(event.isRawData()) {
            // parse the data;
            BinaryEventData eData = event.getData();
            TableMapEventDataParser parser = new TableMapEventDataParser();
            TableMapEventData data =  parser.parse(new ByteArrayInputStream(new java.io.ByteArrayInputStream(eData.getData())));
            Assert.assertEquals(data.getTableID(), 33);
            Assert.assertEquals(data.getDbName(), "test");
            Assert.assertEquals(data.getTableName(), "test_log");
            Assert.assertEquals(data.getMetaDataLength(), 2);
        }
    }
}
