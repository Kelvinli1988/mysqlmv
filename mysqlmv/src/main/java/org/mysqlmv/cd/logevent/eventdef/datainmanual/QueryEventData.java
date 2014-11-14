package org.mysqlmv.cd.logevent.eventdef.datainmanual;

import org.mysqlmv.cd.logevent.EventData;

import java.util.Map;

/**
 * Created by Kelvin Li on 11/13/2014 2:15 PM.
 */
public class QueryEventData implements EventData {
    /*
    +=========================+
    |  Fixed data part        |
    +=========================+
    */
    /**
     * 4 bytes. The ID of the thread that issued this statement. Needed for temporary tables.
     * This is also useful for a DBA for knowing who did what on the master.
     */
    private long threadId;

    /**
     * 4 bytes. The time in seconds that the statement took to execute. Only useful for inspection by the DBA.
     */
    private long timestamp;

    /**
     * 1 byte. The length of the name of the database which was the default database when the statement was executed.
     * This name appears later, in the variable data part. It is necessary for statements such as
     * INSERT INTO t VALUES(1) that don't specify the database and rely on the default database previously selected by
     * USE.
     */
    private int dbNameLength;

    /**
     * 2 bytes. The error code resulting from execution of the statement on the master.
     * Error codes are defined in include/mysqld_error.h. 0 means no error. How come statements with a
     * nonzero error code can exist in the binary log? This is mainly due to the use of nontransactional
     * tables within transactions. For example, if an INSERT ... SELECT fails after inserting 1000 rows
     * into a MyISAM table (for example, with a duplicate-key violation), we have to write this statement
     * to the binary log, because it truly modified the MyISAM table. For transactional tables, there should
     * be no event with a nonzero error code (though it can happen, for example if the connection was interrupted
     * (Control-C)). The slave checks the error code: After executing the statement itself, it compares the error
     * code it got with the error code in the event, and if they are different it stops replicating (unless
     * --slave-skip-errors was used to ignore the error).
     */
    private int errCode;

    /**
     * 2 bytes (not present in v1, v3). The length of the status variable block.
     */
    private int statusVariableBlockLength;

    /*
    +=========================+
    |  Variable data part     |
    +=========================+
     */

    // TODO
    private Map<QueryVariableType, Object> statusVariables;

    /**
     * The default database name (null-terminated).
     */
    private String defaultDBName;
    /**
     * The SQL statement. The slave knows the size of the other fields in the variable part
     * (the sizes are given in the fixed data part), so by subtraction it can know the size of the statement.
     */
    private String sqlStr;

    private enum QueryVariableType {
        /**
         * Value is a 4-byte bit-field. This variable is written only as of MySQL 5.0.
         */
        Q_FLAGS2_CODE(4),

        /**
         * Value is an 8-byte SQL mode value.
         */
        Q_SQL_MODE_CODE(8),

        /**
         * Value is the catalog name: a length byte followed by that many bytes, plus a terminating null byte.
         * This variable is present only in MySQL 5.0.0 to 5.0.3. It was replaced with Q_CATALOG_NZ_CODE in MySQL 5.0.4
         * because the terminating null is unnecessary.
         */
        Q_CATALOG_CODE(-1),
        /**
         * Value is two 2-byte unsigned integers representing the auto_increment_increment and auto_increment_offset system variables.
         * This variable is present only if auto_increment is greater than 1.
         */
        Q_AUTO_INCREMENT(4),
        /**
         *  Value is three 2-byte unsigned integers representing the character_set_client, collation_connection,
         *  and collation_server system variables.
         */
        Q_CHARSET_CODE(6),
        /**
         * Value is the time zone name: a length byte followed by that many bytes.
         * This variable is present only if the time zone string is non-empty.
         */
        Q_TIME_ZONE_CODE(-1),
        /**
         * Value is the catalog name: a length byte followed by that many bytes.
         * Value is always std. This variable is present only if the catalog name is non-empty.
         */
        Q_CATALOG_NZ_CODE(-1),
        /**
         * Value is a 2-byte unsigned integer representing the lc_time_names number.
         * This variable is present only if the value is not 0 (that is, not en_US).
         */
        Q_LC_TIME_NAMES_CODE(2),
        /**
         * Value is a 2-byte unsigned integer representing the collation_database system variable.
         */
        Q_CHARSET_DATABASE_CODE(2),
        /**
         *  Value is 8 bytes representing the table map to be updated by a multiple-table update statement.
         *  Each bit of this variable represents a table, and is set to 1 if the corresponding table is
         *  to be updated by the statement.
         *  Table_map_for_update is used to evaluate the filter rules specified by --replicate-do-table / --replicate-ignore-table.
         */
        Q_TABLE_MAP_FOR_UPDATE_CODE(9)
        ;
        private QueryVariableType(int varLen) {
            QueryVariableType.this.length = varLen;
        }

        public int getLength() {
            return length;
        }

        private int length;
    }

    public long getThreadId() {
        return threadId;
    }

    public void setThreadId(long threadId) {
        this.threadId = threadId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int getDbNameLength() {
        return dbNameLength;
    }

    public void setDbNameLength(int dbNameLength) {
        this.dbNameLength = dbNameLength;
    }

    public int getErrCode() {
        return errCode;
    }

    public void setErrCode(int errCode) {
        this.errCode = errCode;
    }

    public int getStatusVariableBlockLength() {
        return statusVariableBlockLength;
    }

    public void setStatusVariableBlockLength(int statusVariableBlockLength) {
        this.statusVariableBlockLength = statusVariableBlockLength;
    }

    public Map<QueryVariableType, Object> getStatusVariables() {
        return statusVariables;
    }

    public void setStatusVariables(Map<QueryVariableType, Object> statusVariables) {
        this.statusVariables = statusVariables;
    }

    public String getDefaultDBName() {
        return defaultDBName;
    }

    public void setDefaultDBName(String defaultDBName) {
        this.defaultDBName = defaultDBName;
    }

    public String getSqlStr() {
        return sqlStr;
    }

    public void setSqlStr(String sqlStr) {
        this.sqlStr = sqlStr;
    }
}
