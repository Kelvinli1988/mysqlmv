package org.mysqlmv.etp;

import org.mysqlmv.etp.scanner.CreateMVScanner;
import org.testng.annotations.Test;

/**
 * Created by Kelvin Li on 11/21/2014 3:49 PM.
 */
public class CreateMVScannerTest {
    @Test
    public void test() {
        CreateMVScanner mvScanner = new CreateMVScanner();
        mvScanner.run();
    }
}
