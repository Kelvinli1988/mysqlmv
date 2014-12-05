package org.mysqlmv.etp;

import org.mysqlmv.cd.workers.LogFileChangeDetector;
import org.mysqlmv.common.util.CollectionUtils;
import org.mysqlmv.etp.context.ToiContext;
import org.mysqlmv.etp.context.ToiEntry;
import org.mysqlmv.etp.context.ToiValue;
import org.mysqlmv.etp.dao.EtpDao;
import org.mysqlmv.etp.scanner.CreateMVScanner;
import org.mysqlmv.etp.worker.RowEventProcessService;
import org.slf4j.Logger;

import java.sql.SQLException;
import java.util.Map;

/**
 * Created by Kelvin Li on 12/3/2014 2:33 PM.
 */
public class EtpEngine {
    public static Logger logger = org.slf4j.LoggerFactory.getLogger(EtpEngine.class);

    private LogFileChangeDetector logFileChangeDetector;

    private CreateMVScanner mvScanner;

    public void init() {
        try {
            initToiContext();
            logger.info("TOI context initialization finished.");
        } catch (SQLException e) {
            logger.error("Error happens when initializing table of interest context.", e);
        }
        initMVScanner();
        logger.info("MV scanner initialization finished.");
        initLogFileChangeDetector();
        logger.info("Log file change detector initialization finished.");
        initRowEventProcessService();
        logger.info("Row event process service initialization finished.");
        logger.info("Etp engine initialization finished.");
    }

    public void start() {
        Thread mvScannerThread = new Thread(mvScanner, "MVScanner");
        Thread logDetectorThread = new Thread(logFileChangeDetector, "Log-detector");
        mvScannerThread.start();
        logger.info("MV scanner started.");
        logDetectorThread.start();
        logger.info("Log file change detector started.");
        logger.info("Etp engine started.");
    }

    private void initToiContext() throws SQLException {
        Map<ToiEntry, ToiValue> toiContext = EtpDao.findToiContext();
        if (!CollectionUtils.isEmpty(toiContext)) {
            for (Map.Entry<ToiEntry, ToiValue> entry : toiContext.entrySet()) {
                ToiContext.addToiEntry(entry.getKey(), entry.getValue());
                logger.info("ToiContext added, entry:<)" + entry.getKey() + ">, value:<" + entry.getValue() + ">");
            }
        }
    }

    private void initMVScanner() {
        mvScanner = new CreateMVScanner();
    }

    private void initLogFileChangeDetector() {
        logFileChangeDetector = new LogFileChangeDetector();
    }

    private void initRowEventProcessService() {
        RowEventProcessService.init();
    }
}
