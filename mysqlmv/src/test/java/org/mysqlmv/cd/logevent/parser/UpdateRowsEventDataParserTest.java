package org.mysqlmv.cd.logevent.parser;

import org.mysqlmv.cd.logevent.BinLogFile;
import org.mysqlmv.cd.logevent.Event;
import org.mysqlmv.cd.logevent.LogEventType;
import org.mysqlmv.cd.logevent.eventdef.data.*;
import org.mysqlmv.cd.logevent.parser.impl.TableMapContext;
import org.mysqlmv.cd.logevent.parser.impl.TableMapEventDataParser;
import org.mysqlmv.cd.logevent.parser.impl.UpdateRowsEventDataParser;
import org.mysqlmv.cd.logevent.parser.impl.WriteRowsEventDataParser;
import org.mysqlmv.common.io.ByteArrayInputStream;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * Created by Kelvin Li on 11/17/2014 5:32 PM.
 */
public class UpdateRowsEventDataParserTest {
    Event event;


    @BeforeClass
    public void switchFile() throws IOException {
        TableMapEventDataParser tableMapParser = new TableMapEventDataParser();
        BinLogFile logFile = new BinLogFile("src/test/resources/PVGN50874064A-bin.000004");
        EventMiner.switchFile(logFile, 4L);
        for (int i = 0; ; i++) {
            Event ee = EventMiner.nextEvent();
            if (ee.getHeader().getEventType().equals(LogEventType.TABLE_MAP)) {
                TableMapEventData tableData = tableMapParser.parse(new ByteArrayInputStream(new java.io.ByteArrayInputStream(((BinaryEventData) (ee.getData())).getData())));
                TableMapContext.addTableMap(tableData.getTableID(), tableData);
            }
            if (i == 7) {
                event = ee;
                break;
            }
        }
        Assert.assertEquals(event.getHeader().getEventType(), LogEventType.UPDATE_ROWS);
        Assert.assertEquals(event.isRawData(), true);
    }

    @Test
    public void parse() throws IOException {
        BinaryEventData eData = event.getData();
        UpdateRowsEventDataParser parser = new UpdateRowsEventDataParser();
        UpdateRowsEventData data = parser.parse(new ByteArrayInputStream(new java.io.ByteArrayInputStream(eData.getData())));
//        Assert.assertNull(data.getColumnUsageAfterUpdate());
        Assert.assertEquals(data.getColumnNum(), 2);
        Assert.assertEquals(data.getRows().size(), 2);
        RowsEventData.Row row = data.getRows().get(0);
        Assert.assertEquals(row.getCells().get(0).getValue(), 5);
        Assert.assertEquals(row.getCells().get(1).getValue(), "mm");
        row = data.getRows().get(1);
        Assert.assertEquals(row.getCells().get(0).getValue(), 5);
        Assert.assertEquals(row.getCells().get(1).getValue(), "dd");
    }
}
