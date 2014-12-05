package org.mysqlmv.etp.worker;

import org.mysqlmv.cd.logevent.Event;
import org.mysqlmv.cd.logevent.eventdef.data.RowsEventData;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Kelvin Li on 12/1/2014 5:16 PM.
 */
public class RowEventProcessService {

    private static boolean isInitialized = false;

    private static ExecutorService service = null;

    static class RowEventProcessorThreadFactory implements ThreadFactory {
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        RowEventProcessorThreadFactory() {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() :
                    Thread.currentThread().getThreadGroup();
            namePrefix = "RowEventProcess-thread-";
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r,
                    namePrefix + threadNumber.getAndIncrement(),
                    0);
            if (t.isDaemon())
                t.setDaemon(false);
            if (t.getPriority() != Thread.NORM_PRIORITY)
                t.setPriority(Thread.NORM_PRIORITY);
            return t;
        }
    }


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
        service = Executors.newFixedThreadPool(1, new RowEventProcessorThreadFactory());
        isInitialized = true;
    }
}
