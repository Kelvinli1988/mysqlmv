package org.mysqlmv.cd.logevent.parser;

import org.mysqlmv.cd.logevent.BinLogFile;
import org.mysqlmv.cd.logevent.Event;
import org.mysqlmv.cd.logevent.LogEventType;
import org.mysqlmv.cd.logevent.eventdef.data.*;
import org.mysqlmv.cd.logevent.parser.impl.DeleteRowsEventDataParser;
import org.mysqlmv.cd.logevent.parser.impl.TableMapContext;
import org.mysqlmv.cd.logevent.parser.impl.TableMapEventDataParser;
import org.mysqlmv.common.io.ByteArrayInputStream;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * Created by Kelvin Li on 11/17/2014 5:43 PM.
 */
public class DeleteRowsEventDataParserTest {
    Event event;


    @BeforeClass
    public void switchFile() throws IOException {
        TableMapEventDataParser tableMapParser = new TableMapEventDataParser();
        BinLogFile logFile = new BinLogFile("src/test/resources/PVGN50874064A-bin.000006");
        EventMiner.switchFile(logFile, 4L);
        for (int i = 0; ; i++) {
            Event ee = EventMiner.nextEvent();
            if (ee.getHeader().getEventType().equals(LogEventType.TABLE_MAP)) {
                TableMapEventData tableData = tableMapParser.parse(new ByteArrayInputStream(new java.io.ByteArrayInputStream(((BinaryEventData) (ee.getData())).getData())));
                TableMapContext.addTableMap(tableData.getTableID(), tableData);
            }
            if (i == 3) {
                event = ee;
                break;
            }
        }
        Assert.assertEquals(event.getHeader().getEventType(), LogEventType.DELETE_ROWS);
        Assert.assertEquals(event.isRawData(), true);
    }

    @Test
    public void parse() throws IOException {
        BinaryEventData eData = event.getData();
        DeleteRowsEventDataParser parser = new DeleteRowsEventDataParser();
        DeleteRowsEventData data = parser.parse(new ByteArrayInputStream(new java.io.ByteArrayInputStream(eData.getData())));
        Assert.assertNull(data.getColumnUsageAfterUpdate());
        Assert.assertEquals(data.getColumnNum(), 2);
        Assert.assertEquals(data.getRows().size(), 1);
        RowsEventData.Row row = data.getRows().get(0);
        Assert.assertEquals(row.getCells().get(0).getValue(), 2);
        Assert.assertEquals(row.getCells().get(1).getValue(), "ll");
    }
}
