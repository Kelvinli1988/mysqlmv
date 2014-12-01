package org.mysqlmv.cd.logevent;

import java.io.IOException;

/**
 * Created by Kelvin Li on 12/1/2014 2:05 PM.
 */
public interface EventProcessor {
    public void processEvent(Event event) throws IOException;
}
