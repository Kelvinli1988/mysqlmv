package org.mysqlmv.cd.logevent.eventdef.datainmanual;

import org.mysqlmv.cd.logevent.EventData;

/**
 * Created by Kelvin Li on 11/13/2014 3:42 PM.
 */

/**
 * This event is never written, so it cannot exist in a binary log file.
 * It was meant for failsafe replication, which has never been implemented.
 */
public class SlaveEventData implements EventData {
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
    // Empty
}
