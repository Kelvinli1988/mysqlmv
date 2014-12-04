package org.mysqlmv.cd.workers;

import org.mysqlmv.cd.workers.impl.LogFileScanStatus;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

/**
 * Created by Kelvin Li on 12/1/2014 1:43 PM.
 */
public interface LogFileChangeProcessor {
    public LogFileScanStatus onFileChange(File logfile, boolean isNewFile) throws SQLException, IOException;
}
