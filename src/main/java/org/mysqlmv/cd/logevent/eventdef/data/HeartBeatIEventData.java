package org.mysqlmv.cd.logevent.eventdef.data;

import org.mysqlmv.cd.logevent.IEventData;

/**
 * Created by Kelvin Li on 11/13/2014 5:28 PM.
 */

/**
 * A Heartbeat_log_event is sent by a master to a slave to let the slave know that the master is still alive.
 * Events of this type do not appear in the binary or relay logs. They are generated on a master server by the
 * thread that dumps events and sent straight to the slave without ever being written to the binary log.
 * The slave examines the event contents and then discards it without writing it to the relay log.
 */
public class HeartBeatIEventData implements IEventData {
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
