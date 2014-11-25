package org.mysqlmv.common.config.reader;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by Kelvin Li on 11/18/2014 2:47 PM.
 */
public class ConfigFactory {
    private static Properties prop;
    private ConfigFactory() {
        prop = new Properties();
        loadConfig();
    }

    private static ConfigFactory INSTANCE;

    public synchronized static ConfigFactory getINSTANCE() {
        if(INSTANCE == null) {
            INSTANCE = new ConfigFactory();
        }
        return INSTANCE;
    }
    private void loadConfig() {
        InputStream in = getClass().getResourceAsStream("/config.properties");
        try {
            prop.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getProperty(String key) {
        return String.valueOf(prop.get(key));
    }
}
