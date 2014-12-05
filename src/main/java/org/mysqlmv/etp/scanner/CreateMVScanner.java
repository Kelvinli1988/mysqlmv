package org.mysqlmv.etp.scanner;

import org.mysqlmv.Switch;
import org.mysqlmv.common.util.CollectionUtils;
import org.mysqlmv.etp.dao.EtpDao;
import org.mysqlmv.etp.mv.MaterializedView;
import org.slf4j.Logger;

import java.util.List;

/**
 * Created by Kelvin Li on 11/21/2014 3:06 PM.
 */
public class CreateMVScanner implements Runnable {
    public static Logger logger = org.slf4j.LoggerFactory.getLogger(CreateMVScanner.class);

    @Override
    public void run() {
        try {
            Switch aswitch = Switch.getSwitch();
            while(aswitch.getStatus()) {
                runTask();
                Thread.sleep(1000L);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void runTask() {
        List<MaterializedView> mvList = EtpDao.findUninitializedMV();
        if(!CollectionUtils.isEmpty(mvList)) {
            for(MaterializedView thisMV : mvList) {
                new MVInitializer(thisMV).initializeMV();
            }
        }
    }
}