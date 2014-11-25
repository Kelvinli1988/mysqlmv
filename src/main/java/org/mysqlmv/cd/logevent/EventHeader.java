package org.mysqlmv.cd.logevent;

import org.mysqlmv.cd.logevent.eventdef.header.EventVersion;

import java.io.Serializable;

/**
 * Created by Kelvin Li on 11/13/2014 10:14 AM.
 */
public interface EventHeader extends Serializable {
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
