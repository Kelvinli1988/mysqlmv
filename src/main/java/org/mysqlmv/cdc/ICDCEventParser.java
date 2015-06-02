package org.mysqlmv.cdc;

import org.mysqlmv.cdc.logevent.ICDCEvent;
import org.mysqlmv.cdc.logevent.ICDCRowSet;

/**
 * Created by I312762 on 6/1/2015.
 */
public interface ICDCEventParser {

    ICDCRowSet parse(ICDCEvent event);
}
