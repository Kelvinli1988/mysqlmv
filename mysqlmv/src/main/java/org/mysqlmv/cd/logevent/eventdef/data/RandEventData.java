package org.mysqlmv.cd.logevent.eventdef.data;

import org.mysqlmv.cd.logevent.EventData;

/**
 * Created by Kelvin Li on 11/13/2014 4:31 PM.
 */

/**
 * RAND() in MySQL generates a random number. A RAND_EVENT contains two seed values that set the rand_seed1 and rand_seed2 system variables that are used to compute the random number.
 */
public class RandEventData implements EventData {
    /*
    +=========================+
    |  Fixed data part        |
    +=========================+
    */
    // empty

    /*
    +=========================+
    |  Variable data part     |
    +=========================+
     */
    /**
     * 8 bytes. The value for the first seed.
     */
    private long firstSeed;
    /**
     * 8 bytes. The value for the second seed.
     */
    private long secondSeed;

}
