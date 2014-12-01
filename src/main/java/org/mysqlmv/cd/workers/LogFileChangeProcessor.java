package org.mysqlmv.cd.workers;

import java.io.File;
import java.sql.SQLException;

/**
 * Created by Kelvin Li on 12/1/2014 1:43 PM.
 */
public interface LogFileChangeProcessor {
    public void onFileChange(File logfile) throws SQLException;
}
