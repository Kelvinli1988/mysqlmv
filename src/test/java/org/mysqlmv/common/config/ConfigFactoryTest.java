package org.mysqlmv.common.config;

import org.mysqlmv.common.config.reader.ConfigFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Created by Kelvin Li on 11/18/2014 2:52 PM.
 */
public class ConfigFactoryTest {
    @Test
    public void test() {
        Assert.assertEquals(ConfigFactory.getINSTANCE().getProperty("log-root-folder"), "C:/ProgramData/MySQL/MySQL Server 5.5/data");
    }
}
