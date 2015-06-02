package org.mysqlmv.cdc.logevent;

import org.mysqlmv.cdc.logevent.schema.ITable;

import java.io.Serializable;

/**
 *
 * Created by Kelvin Li on 6/1/2015.
 */
public interface ICDCRowSet extends Serializable {

    /**
     * Get table related information of this rowset.
     * @return
     */
    ITable getTable();

    /**
     *
     * @return the time the transaction committed.
     */
    long getTransactionTime();

    /**
     * If it is a insert, this will return the data;
     * If it is a update, this will return the data before the update;
     * If it is a delete, this will return null.
     * @return
     */
    IRow getBeforeRow();

    /**
     * If it is a insert, this will return null;
     * If it is a update, this will return the data after the update;
     * If it is a delete, this will return the data.
     * @return
     */
    IRow getAfterRow();
}
