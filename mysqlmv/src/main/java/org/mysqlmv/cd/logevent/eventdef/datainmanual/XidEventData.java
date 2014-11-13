package org.mysqlmv.cd.logevent.eventdef.datainmanual;

import org.mysqlmv.cd.logevent.EventData;

/**
 * Created by Kelvin Li on 11/13/2014 4:49 PM.
 */

/**
 * An XID event is generated for a commit of a transaction that modifies one or more tables of an XA-capable storage engine. Strictly speaking, Xid_log_event is used if thd->transaction.xid_state.xid.get_my_xid() returns nonzero.

 Here is an example of how to generate an XID event (it occurs whether or not innodb_support_xa is enabled):

 CREATE TABLE t1 (a INT) ENGINE = INNODB;
 START TRANSACTION;
 INSERT INTO t1 VALUES (1);
 COMMIT;
 */
public class XidEventData implements EventData {
    /*
    +=========================+
    |  Fixed data part        |
    +=========================+
    */
    // Empty

    /*
    +=========================+
    |  Variable data part     |
    +=========================+
     */
    /**
     * 8 bytes. The XID transaction number.
     */
    private long transactionXid;

    public long getTransactionXid() {
        return transactionXid;
    }

    public void setTransactionXid(long transactionXid) {
        this.transactionXid = transactionXid;
    }
}