package org.mysqlmv.etp;

import org.mysqlmv.Switch;
import org.mysqlmv.etp.scanner.CreateMVScanner;
import org.testng.annotations.Test;

/**
 * Created by Kelvin Li on 11/21/2014 3:49 PM.
 */
public class CreateMVScannerTest {
    @Test
    public void test() {
        Switch sw = Switch.getSwitch();
        sw.startup();
        new CreateMVScanner().run();
//        Thread mvScanner = new Thread(new CreateMVScanner());
//        mvScanner.start();
    }
}
