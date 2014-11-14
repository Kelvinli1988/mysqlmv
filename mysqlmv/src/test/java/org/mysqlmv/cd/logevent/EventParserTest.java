package org.mysqlmv.cd.logevent;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;

/**
 * Created by Kelvin Li on 11/13/2014 1:16 PM.
 */
public class EventParserTest {

    @Test
    public void guaranteeFileExist() {
        File binFile = new File("src/test/resources/PVGN50874064A-bin.000001");
        Assert.assertTrue(binFile.exists());
        Assert.assertTrue(binFile.isFile());
    }

    @Test
    public void parseBinFile() throws IOException {
        File ff = new File("src/test/resources/PVGN50874064A-bin.000001");
        BinLogFile binLog = new BinLogFile("src/test/resources/PVGN50874064A-bin.000001");
        Assert.assertTrue(binLog.validateLogFile());
    }
}
