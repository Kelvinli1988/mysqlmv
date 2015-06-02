package org.mysqlmv.cdc;

import org.mysqlmv.cdc.logevent.IEvent;

/**
 * Created by Kelvin Li on 6/1/2015.
 */
public interface ILogScanner {

    /**
     * Set the start point of the log scanner.
     * @param locator
     */
    public void setLocator(ILocator locator);

    /**
     * Get the next event from the log file.
     */
    public IEvent getNextEvent();
}
