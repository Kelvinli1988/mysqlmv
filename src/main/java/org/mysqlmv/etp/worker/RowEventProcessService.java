package org.mysqlmv.etp.worker;

import org.mysqlmv.cd.logevent.Event;
import org.mysqlmv.cd.logevent.eventdef.data.RowsEventData;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Kelvin Li on 12/1/2014 5:16 PM.
 */
public class RowEventProcessService {

    private static boolean isInitialized = false;

    private static ExecutorService service = null;

    public static void submitRowEvent(Event event) {
        if(!isInitialized) {
            throw new RuntimeException("Row event process service not initialized yet.");
        }
        if(event.getData() instanceof RowsEventData) {
            service.submit(new RowEventProcessor(event));
        } else {
            // TODO add some logger;
        }
    }

    public static void init() {
        if(isInitialized) {
            return;
        }
        service = Executors.newFixedThreadPool(1);
        isInitialized = true;
    }
}
