package org.mysqlmv.etp;

import org.mysqlmv.cd.workers.LogFileChangeDetector;
import org.mysqlmv.common.io.db.DBUtil;
import org.mysqlmv.common.io.db.QueryCallBack;
import org.mysqlmv.etp.context.ToiContext;
import org.mysqlmv.etp.context.ToiEntry;
import org.mysqlmv.etp.context.ToiValue;
import org.mysqlmv.etp.scanner.CreateMVScanner;
import org.mysqlmv.etp.worker.RowEventProcessService;
import org.slf4j.Logger;

import java.sql.PreparedStatement;
import java.sql.SQLException;

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
        } catch (SQLException e) {
            logger.error("Error happens when initializing table of interest context.", e);
        }
        initMVScanner();
        initLogFileChangeDetector();
        initRowEventProcessor();
    }

    private void initToiContext() throws SQLException {
        DBUtil.executeInPrepareStmt(new QueryCallBack() {
            @Override
            public String getSql() {
                return "select mview_toi_id, mview_id, schema_name, table_name " +
                "from mview_toi where setup_finished = 1 order by schema_name, table_name";
            }

            @Override
            public Object doInCallback(PreparedStatement pstmt) throws SQLException {
                pstmt.execute();
                rs = pstmt.getResultSet();
                while(rs.next()) {
                    String schema = rs.getString("schema_name");
                    String table = rs.getString("table_name");
                    int mview_toi_id = rs.getInt("mview_toi_id");
                    int mview_id = rs.getInt("mview_id");
                    ToiContext.addToiEntry(new ToiEntry(schema, table), new ToiValue(mview_toi_id, mview_id));
                }
                return null;
            }
        });
    }

    private void initMVScanner() {
        mvScanner = new CreateMVScanner();
    }

    private void initLogFileChangeDetector() {
        logFileChangeDetector = new LogFileChangeDetector();
    }

    private void initRowEventProcessor() {
        RowEventProcessService.init();
    }
}
