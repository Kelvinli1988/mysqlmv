package org.mysqlmv.cd.logevent;

import java.io.Serializable;

/**
 * Created by I312762 on 6/1/2015.
 */
public interface IEvent extends Serializable {
    /**
     * Get the data part of this event.
     * @return
     */
    IEventData getData();

    /**
     * Get the header part of this event.
     * @return
     */
    IEventHeader getHeader();
}
