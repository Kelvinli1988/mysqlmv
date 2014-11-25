package org.mysqlmv.cd.logevent;

/**
 * Created by Kelvin Li on 11/18/2014 2:36 PM.
 */
public interface Switchable {
    boolean switchFile(String newFile, long skip);
}
