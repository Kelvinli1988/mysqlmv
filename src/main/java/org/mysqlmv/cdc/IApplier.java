package org.mysqlmv.cdc;

import org.mysqlmv.cdc.logevent.ICDCRowSet;

/**
 * This class will apply change set to relative delta table.
 *
 * Created by Kelvin Li on 6/1/2015.
 */

public interface IApplier {

    public String apply(ICDCRowSet rowSet);
}
