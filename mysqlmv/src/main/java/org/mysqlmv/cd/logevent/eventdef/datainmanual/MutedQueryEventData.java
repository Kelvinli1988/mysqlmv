package org.mysqlmv.cd.logevent.eventdef.datainmanual;

/**
 * Created by Kelvin Li on 11/13/2014 5:29 PM.
 */

/**
 * This is a subclass of Query_log_event that is not written to the log. It is used as a means of flushing a transaction without logging an event.

 This event class was added in MySQL 5.0.23 and removed in 6.0.4. It was a solution to Bug#16206 that became unnecessary with the fix for Bug#29020.
 */
public class MutedQueryEventData extends QueryEventData {
}
