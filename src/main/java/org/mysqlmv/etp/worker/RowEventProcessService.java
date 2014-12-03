package org.mysqlmv.etp.worker;

import org.mysqlmv.cd.logevent.Event;
import org.mysqlmv.cd.logevent.eventdef.data.RowsEventData;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Kelvin Li on 12/1/2014 5:16 PM.
 */
public class RowEventProcessService {
    private static ExecutorService service = Executors.newFixedThreadPool(2);

    public static void submitRowEvent(Event event) {
        if(event.getData() instanceof RowsEventData) {
            service.submit(new RowEventProcessor(event));
        } else {
            // TODO add some logger;
        }
    }
}
