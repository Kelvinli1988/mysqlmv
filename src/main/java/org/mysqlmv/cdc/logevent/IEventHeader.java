package org.mysqlmv.cdc.logevent;

import org.mysqlmv.cd.logevent.LogEventType;
import org.mysqlmv.cd.logevent.eventdef.header.EventVersion;

import java.io.Serializable;

/**
 * Created by Kelvin Li on 11/13/2014 10:14 AM.
 */
public interface IEventHeader extends Serializable {
    long getTimestamp();

    LogEventType getEventType();

    long getServerId();

    int getHeaderLength();

    int getDataLength();

    long getNextPosition();

    int getFlag();

    byte[] getExtraHeader();

    EventVersion getVersion();
}
