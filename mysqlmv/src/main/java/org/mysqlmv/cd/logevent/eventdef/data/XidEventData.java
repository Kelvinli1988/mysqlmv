package org.mysqlmv.cd.logevent.eventdef.data;

import org.mysqlmv.cd.logevent.EventData;

/**
 * Created by Kelvin Li on 11/13/2014 4:49 PM.
 */

/**
 * An XID event is generated for a commit of a transaction that modifies one or more tables of an XA-capable storage engine. Strictly speaking, Xid_log_event is used if thd->transaction.xid_state.xid.get_my_xid() returns nonzero.
 * <p/>
 * Here is an example of how to generate an XID event (it occurs whether or not innodb_support_xa is enabled):
 * <p/>
 * CREATE TABLE t1 (a INT) ENGINE = INNODB;
 * START TRANSACTION;
 * INSERT INTO t1 VALUES (1);
 * COMMIT;
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
     * <p/>
     * Note: Contrary to all other numeric fields, the XID transaction number is not always written in little-endian format. The bytes are copied unmodified from memory to disk, so the format is machine-dependent. Hence, when replicating from a little-endian to a big-endian machine (or vice versa), the numeric value of transaction numbers will differ. In particular, the output of mysqlbinlog differs. This should does not cause inconsistencies in replication because the only important property of transaction numbers is that different transactions have different numbers (relative order does not matter).
     */
    private long xid;

    public long getXid() {
        return xid;
    }

    public void setXid(long xid) {
        this.xid = xid;
    }

    @Override
    public String toString() {
        return "{xid = " + xid + "}";
    }
}